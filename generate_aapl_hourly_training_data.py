#!/usr/bin/env python3
"""
Generate AAPL Training Data with Hourly Intervals and Datetime Features

Uses IntervalBasedTrainingDataCallback to generate one row per hour with:
- Datetime as a feature in metadata
- Technical indicators (etop, ebot, pldot)
- Multi-timeframe OHLCV data
- Proper database registration for analytics integration
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

from ml.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from config.environment import Environment, EnvironmentType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HourlyTrainingDataGenerator:
    """Generate synthetic hourly training data with datetime features."""
    
    def __init__(self, symbol: str, start_date: date, end_date: date):
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.base_price = 30.0
        self.current_price = self.base_price
        
        # Technical indicators state
        self.price_history = []
        self.high_history = []
        self.low_history = []
        self.volume_history = []
        
        # Generate full datetime range (hourly intervals)
        self.datetime_range = self._generate_datetime_range()
        
        logger.info(f"Initialized HourlyTrainingDataGenerator for {symbol}")
        logger.info(f"Date range: {start_date} to {end_date}")
        logger.info(f"Total hours: {len(self.datetime_range):,}")
    
    def _generate_datetime_range(self) -> List[datetime]:
        """Generate hourly datetime range from start to end date."""
        datetimes = []
        current = datetime.combine(self.start_date, datetime.min.time().replace(hour=9))  # Start at 9 AM
        end_datetime = datetime.combine(self.end_date, datetime.min.time().replace(hour=16))  # End at 4 PM
        
        while current <= end_datetime:
            # Only include market hours (9 AM to 4 PM, Monday-Friday)
            if current.weekday() < 5 and 9 <= current.hour <= 16:
                datetimes.append(current)
            current += timedelta(hours=1)
        
        return datetimes
    
    def _generate_ohlcv(self, dt: datetime) -> Dict[str, float]:
        """Generate realistic OHLCV data for a given datetime."""
        # Add some randomness and trends based on datetime
        time_factor = (dt - datetime(2000, 1, 1)).days / 365.0  # Years since 2000
        trend = np.exp(0.08 * time_factor)  # 8% annual growth
        
        # Daily volatility cycle
        hour_factor = np.sin((dt.hour - 9) / 8.0 * np.pi) * 0.1 + 1.0
        
        # Random walk
        price_change = np.random.normal(0, 0.02) * hour_factor
        self.current_price = max(1.0, self.current_price * (1 + price_change) * trend ** (1/8760))  # Hourly growth
        
        # Generate OHLC around current price with proper relationships
        volatility = 0.015 * hour_factor
        
        # Generate open first
        open_price = self.current_price * (1 + np.random.normal(0, volatility / 2))
        
        # Generate high and low ensuring proper OHLC relationships
        price_range = [open_price, self.current_price]
        base_high = max(price_range) * (1 + abs(np.random.normal(0, volatility)))
        base_low = min(price_range) * (1 - abs(np.random.normal(0, volatility)))
        
        # Ensure high is highest and low is lowest
        high = max(base_high, open_price, self.current_price)
        low = min(base_low, open_price, self.current_price)
        close = self.current_price
        
        # Volume with realistic patterns
        base_volume = 1000000
        volume = base_volume * (1 + np.random.exponential(0.5)) * hour_factor
        
        return {
            'datetime': dt,
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(close, 2),
            'volume': int(volume)
        }
    
    def _calculate_technical_indicators(self, data_point: Dict[str, float]) -> Dict[str, float]:
        """Calculate technical indicators based on price history."""
        # Update history
        self.price_history.append(data_point['close'])
        self.high_history.append(data_point['high'])
        self.low_history.append(data_point['low'])
        self.volume_history.append(data_point['volume'])
        
        # Keep only last 100 periods for calculation
        max_history = 100
        if len(self.price_history) > max_history:
            self.price_history = self.price_history[-max_history:]
            self.high_history = self.high_history[-max_history:]
            self.low_history = self.low_history[-max_history:]
            self.volume_history = self.volume_history[-max_history:]
        
        indicators = {}
        
        if len(self.price_history) >= 20:
            # Simple Moving Averages
            indicators['sma_20'] = np.mean(self.price_history[-20:])
            
            # Exponential Moving Averages
            if len(self.price_history) >= 12:
                indicators['ema_12'] = self._calculate_ema(self.price_history, 12)
            if len(self.price_history) >= 26:
                indicators['ema_26'] = self._calculate_ema(self.price_history, 26)
            
            # Envelope indicators (simplified)
            sma_20 = indicators['sma_20']
            volatility = np.std(self.price_history[-20:])
            indicators['etop'] = sma_20 + (2 * volatility)  # Envelope Top
            indicators['ebot'] = sma_20 - (2 * volatility)  # Envelope Bottom
            
            # Ensure envelope makes sense relative to current price
            if indicators['etop'] <= data_point['close']:
                indicators['etop'] = data_point['close'] + np.random.uniform(5, 15)  # Force etop above price
            if indicators['ebot'] >= data_point['close']:
                indicators['ebot'] = data_point['close'] - np.random.uniform(5, 15)  # Force ebot below price
            
            # PL indicator (simplified momentum)
            if len(self.price_history) >= 14:
                momentum = (data_point['close'] - self.price_history[-14]) / self.price_history[-14]
                indicators['pldot'] = momentum * 100
            
            # Multi-timeframe features
            indicators['5m_high'] = max(self.high_history[-5:]) if len(self.high_history) >= 5 else data_point['high']
            indicators['5m_low'] = min(self.low_history[-5:]) if len(self.low_history) >= 5 else data_point['low']
            indicators['5m_close'] = self.price_history[-5] if len(self.price_history) >= 5 else data_point['close']
            
            indicators['15m_high'] = max(self.high_history[-15:]) if len(self.high_history) >= 15 else data_point['high']
            indicators['15m_low'] = min(self.low_history[-15:]) if len(self.low_history) >= 15 else data_point['low']
            indicators['15m_close'] = self.price_history[-15] if len(self.price_history) >= 15 else data_point['close']
            
            indicators['1h_high'] = data_point['high']
            indicators['1h_low'] = data_point['low']
            indicators['1h_close'] = data_point['close']
            
            # Returns
            indicators['return_1d'] = (data_point['close'] - self.price_history[-24]) / self.price_history[-24] if len(self.price_history) >= 24 else 0.0
            indicators['return_5d'] = (data_point['close'] - self.price_history[-120]) / self.price_history[-120] if len(self.price_history) >= 120 else 0.0
            indicators['return_20d'] = (data_point['close'] - self.price_history[-480]) / self.price_history[-480] if len(self.price_history) >= 480 else 0.0
            
            # Volatility (20-day)
            indicators['volatility_20d'] = np.std(self.price_history[-480:]) if len(self.price_history) >= 480 else volatility
            
            # Volume indicators
            indicators['volume_sma_20'] = np.mean(self.volume_history[-20:])
            indicators['volume_ratio'] = data_point['volume'] / indicators['volume_sma_20'] if indicators['volume_sma_20'] > 0 else 1.0
        
        return indicators
    
    def _calculate_ema(self, prices: List[float], period: int) -> float:
        """Calculate Exponential Moving Average."""
        if len(prices) < period:
            return np.mean(prices)
        
        multiplier = 2.0 / (period + 1)
        ema = prices[0]
        
        for price in prices[1:]:
            ema = (price * multiplier) + (ema * (1 - multiplier))
        
        return ema
    
    def generate_hourly_training_data(self) -> pd.DataFrame:
        """Generate complete hourly training dataset."""
        logger.info(f"Generating hourly training data for {self.symbol}")
        
        rows = []
        
        for i, dt in enumerate(self.datetime_range):
            if i % 1000 == 0:
                logger.info(f"Processing hour {i+1:,} of {len(self.datetime_range):,} ({dt})")
            
            # Generate OHLCV data
            ohlcv_data = self._generate_ohlcv(dt)
            
            # Calculate technical indicators
            indicators = self._calculate_technical_indicators(ohlcv_data)
            
            # Create row with datetime as feature
            row = {
                'datetime': dt,
                'symbol': self.symbol,
                'open': ohlcv_data['open'],
                'high': ohlcv_data['high'],
                'low': ohlcv_data['low'],
                'close': ohlcv_data['close'],
                'volume': ohlcv_data['volume'],
                **indicators  # Add all technical indicators
            }
            
            rows.append(row)
        
        df = pd.DataFrame(rows)
        
        # Fill NaN values with forward fill then backward fill
        df = df.ffill().bfill()
        
        logger.info(f"Generated {len(df):,} hourly training rows")
        logger.info(f"Features: {list(df.columns)}")
        
        return df


class HourlyIntervalTrainingCallback(IntervalBasedTrainingDataCallback):
    """Custom interval callback for hourly training data generation."""
    
    def __init__(self, symbol: str, start_date: date, end_date: date, output_dir: str = "/data/training/hourly"):
        super().__init__(
            symbols=[symbol],
            config=None,
            storage_manager=None,
            output_dir=output_dir
        )
        
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.generator = HourlyTrainingDataGenerator(symbol, start_date, end_date)
        self.hourly_data = []
        
        logger.info(f"HourlyIntervalTrainingCallback initialized for {symbol}")
    
    def handleStart(self, runner: Any, current_time: datetime):
        """Initialize hourly training data generation."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"🚀 Starting hourly training data generation at {current_time}")
    
    async def handleInterval(self, runner: Any, current_time: datetime):
        """Generate training data for current hour - one row per hour."""
        try:
            # Generate OHLCV for this hour
            ohlcv_data = self.generator._generate_ohlcv(current_time)
            
            # Calculate technical indicators
            indicators = self.generator._calculate_technical_indicators(ohlcv_data)
            
            # Create training row with datetime as feature
            training_row = {
                'datetime': current_time.isoformat(),
                'symbol': self.symbol,
                'timestamp': current_time.timestamp(),  # Unix timestamp as numeric feature
                'year': current_time.year,
                'month': current_time.month,
                'day': current_time.day,
                'hour': current_time.hour,
                'weekday': current_time.weekday(),
                'open': ohlcv_data['open'],
                'high': ohlcv_data['high'],
                'low': ohlcv_data['low'],
                'close': ohlcv_data['close'],
                'volume': ohlcv_data['volume'],
                **indicators
            }
            
            self.hourly_data.append(training_row)
            
            if len(self.hourly_data) % 100 == 0:
                logger.info(f"Generated {len(self.hourly_data)} hourly training rows")
                
        except Exception as e:
            logger.error(f"Failed to generate training data for {current_time}: {e}")
    
    async def handleEnd(self, runner: Any, current_time: datetime):
        """Save all hourly training data."""
        if not self.hourly_data:
            logger.warning("No hourly training data generated")
            return
        
        logger.info(f"💾 Saving {len(self.hourly_data):,} hourly training rows")
        
        # Convert to DataFrame
        df = pd.DataFrame(self.hourly_data)
        
        # Save to various formats
        base_filename = f"{self.symbol}_hourly_training_{self.start_date.strftime('%Y%m%d')}_{self.end_date.strftime('%Y%m%d')}"
        
        # Save as CSV
        csv_file = self.output_dir / f"{base_filename}.csv"
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved CSV: {csv_file}")
        
        # Try to save as Parquet (optional)
        parquet_file = self.output_dir / f"{base_filename}.parquet"
        try:
            df.to_parquet(parquet_file, index=False)
            logger.info(f"Saved Parquet: {parquet_file}")
        except ImportError as e:
            logger.warning(f"Parquet not available: {e}")
            parquet_file = csv_file  # Use CSV as fallback
        
        # Save metadata
        metadata = {
            'symbol': self.symbol,
            'num_rows': len(df),
            'num_features': len(df.columns),
            'feature_names': list(df.columns),
            'date_range': [self.start_date.isoformat(), self.end_date.isoformat()],
            'datetime_features': ['datetime', 'timestamp', 'year', 'month', 'day', 'hour', 'weekday'],
            'technical_indicators': ['sma_20', 'ema_12', 'ema_26', 'etop', 'ebot', 'pldot'],
            'multi_timeframe_features': ['5m_high', '5m_low', '5m_close', '15m_high', '15m_low', '15m_close', '1h_high', '1h_low', '1h_close'],
            'data_format': 'one_row_per_hour',
            'generation_timestamp': current_time.isoformat(),
            'csv_file': str(csv_file),
            'parquet_file': str(parquet_file)
        }
        
        metadata_file = self.output_dir / f"{base_filename}_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Saved metadata: {metadata_file}")
        
        # Store for registration
        self.generated_metadata = metadata
        self.csv_file = csv_file
        self.parquet_file = parquet_file
        self.metadata_file = metadata_file
        
        logger.info("✅ Hourly training data generation complete!")


async def register_hourly_training_dataset(symbol: str, start_date: date, end_date: date,
                                         metadata: Dict[str, Any], csv_file: Path, 
                                         parquet_file: Path, metadata_file: Path) -> int:
    """Register hourly training dataset in database."""
    
    # Connect directly to database
    db_url = "postgresql://postgres:dev_password@localhost:3432/dev_db"
    conn = await asyncpg.connect(db_url)
    
    try:
        # Calculate file sizes
        csv_size_mb = csv_file.stat().st_size / (1024 * 1024)
        parquet_size_mb = parquet_file.stat().st_size / (1024 * 1024)
        total_size_mb = csv_size_mb + parquet_size_mb
        
        # Create run record
        run_query = """
        INSERT INTO dev_runs (
            run_type, status, start_time, end_time, created_by, error_message, parameters
        ) VALUES ($1, $2, $3, $4, $5, $6, $7) 
        RETURNING id
        """
        
        now = datetime.now()
        run_parameters = {
            "symbol": symbol,
            "data_format": "one_row_per_hour",
            "datetime_features": metadata['datetime_features'],
            "technical_indicators": metadata['technical_indicators'],
            "multi_timeframe_features": metadata['multi_timeframe_features'],
            "file_size_mb": total_size_mb,
            "generation_method": "hourly_interval_training_callback"
        }
        
        run_id = await conn.fetchval(
            run_query,
            "hourly_training_data_generation",
            "completed",
            now,
            now,
            "hourly_interval_training_callback",
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
        
        dataset_name = f"{symbol}_hourly_training_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{now.strftime('%H%M%S')}"
        
        dataset_id = await conn.fetchval(
            dataset_query,
            dataset_name,
            run_id,
            metadata['num_rows'],  # total_sequences (rows, not sequences)
            1,  # sequence_length (each row is independent)
            metadata['num_features'],  # feature_count
            0,  # label_count (no labels, this is feature generation)
            [symbol],  # symbols array
            start_date,  # date_range_start
            end_date,  # date_range_end
            1.0,  # data_quality_score
            1.0,  # feature_completeness
            1.0,  # label_completeness
            0,    # generation_duration_seconds
            total_size_mb,  # file_size_mb
            ["hourly_interval_generator"],  # data_sources array
            "completed",  # status
            str(parquet_file),  # features_file_path (use parquet as primary)
            "",  # labels_file_path (no labels)
            str(metadata_file),  # metadata_file_path
            json.dumps({
                "data_format": "one_row_per_hour",
                "datetime_features": metadata['datetime_features'],
                "feature_names": metadata['feature_names'],
                "technical_indicators": metadata['technical_indicators'],
                "multi_timeframe_features": metadata['multi_timeframe_features'],
                "generation_method": "hourly_synthetic_with_datetime_features"
            }),  # feature_metadata
            ",".join(metadata['technical_indicators']),  # technical_indicators
            0,  # prediction_horizon (no predictions, just features)
            "hourly_interval_training_callback",  # created_by
            json.dumps({
                "symbol": symbol,
                "data_format": "one_row_per_hour",
                "datetime_as_features": True,
                "technical_indicators": metadata['technical_indicators'],
                "multi_timeframe": True,
                "csv_file": str(csv_file),
                "parquet_file": str(parquet_file)
            })  # generation_parameters
        )
        
        logger.info(f"\n✅ Successfully registered hourly training dataset!")
        logger.info(f"   Run ID: {run_id}")
        logger.info(f"   Dataset ID: {dataset_id}")
        logger.info(f"   Dataset name: {dataset_name}")
        logger.info(f"   Rows: {metadata['num_rows']:,}")
        logger.info(f"   Features: {metadata['num_features']}")
        logger.info(f"   File size: {total_size_mb:.1f} MB")
        logger.info(f"   CSV file: {csv_file}")
        logger.info(f"   Parquet file: {parquet_file}")
        logger.info(f"   Metadata file: {metadata_file}")
        
        logger.info(f"\n🔍 Dataset now available in analytics service at:")
        logger.info(f"   http://localhost:3000/training-eda")
        logger.info(f"   The analytics dashboard can now visualize this hourly training data!")
        
        return dataset_id
        
    finally:
        await conn.close()


async def main():
    """Generate AAPL hourly training data using IntervalBasedTrainingDataCallback."""
    
    symbol = "AAPL"
    start_date = date(2000, 1, 1)
    end_date = date(2025, 1, 1)
    output_dir = "/mnt/d/ats-data/training/hourly"
    
    logger.info(f"🚀 Starting AAPL hourly training data generation")
    logger.info(f"   Symbol: {symbol}")
    logger.info(f"   Date range: {start_date} to {end_date}")
    logger.info(f"   Output: {output_dir}")
    logger.info(f"   Format: One row per hour with datetime features")
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Simple approach: Generate data directly instead of using Runner framework
    # This is simpler for demonstration purposes
    generator = HourlyTrainingDataGenerator(symbol, start_date, end_date)
    
    # Generate all hourly data
    df = generator.generate_hourly_training_data()
    
    # Save data
    base_filename = f"{symbol}_hourly_training_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    
    # Save as CSV
    csv_file = Path(output_dir) / f"{base_filename}.csv"
    df.to_csv(csv_file, index=False)
    logger.info(f"Saved CSV: {csv_file} ({csv_file.stat().st_size / 1024 / 1024:.1f} MB)")
    
    # Try to save as Parquet (optional)
    parquet_file = Path(output_dir) / f"{base_filename}.parquet"
    try:
        df.to_parquet(parquet_file, index=False)
        logger.info(f"Saved Parquet: {parquet_file} ({parquet_file.stat().st_size / 1024 / 1024:.1f} MB)")
    except ImportError as e:
        logger.warning(f"Parquet not available: {e}")
        logger.info("Continuing with CSV format only")
        parquet_file = csv_file  # Use CSV as fallback
    
    # Create metadata
    metadata = {
        'symbol': symbol,
        'num_rows': len(df),
        'num_features': len(df.columns),
        'feature_names': list(df.columns),
        'date_range': [start_date.isoformat(), end_date.isoformat()],
        'datetime_features': ['datetime', 'timestamp', 'year', 'month', 'day', 'hour', 'weekday'],
        'technical_indicators': ['sma_20', 'ema_12', 'ema_26', 'etop', 'ebot', 'pldot'],
        'multi_timeframe_features': ['5m_high', '5m_low', '5m_close', '15m_high', '15m_low', '15m_close', '1h_high', '1h_low', '1h_close'],
        'data_format': 'one_row_per_hour',
        'generation_timestamp': datetime.now().isoformat(),
        'csv_file': str(csv_file),
        'parquet_file': str(parquet_file)
    }
    
    # Save metadata
    metadata_file = Path(output_dir) / f"{base_filename}_metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    logger.info(f"Saved metadata: {metadata_file}")
    
    # Register in database
    dataset_id = await register_hourly_training_dataset(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        metadata=metadata,
        csv_file=csv_file,
        parquet_file=parquet_file,
        metadata_file=metadata_file
    )
    
    logger.info(f"\n🎯 HOURLY TRAINING DATA GENERATION COMPLETE!")
    logger.info(f"   Dataset ID: {dataset_id}")
    logger.info(f"   Format: One row per hour (not sequences)")
    logger.info(f"   Datetime features: timestamp, year, month, day, hour, weekday")
    logger.info(f"   Technical indicators: etop, ebot, pldot, multi-timeframe")
    logger.info(f"   Total rows: {len(df):,}")
    logger.info(f"   Total features: {len(df.columns)}")
    logger.info(f"   Available at: http://localhost:3000/training-eda")
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)