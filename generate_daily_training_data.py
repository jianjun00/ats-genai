#!/usr/bin/env python3
"""
Generate Daily Training Data for AAPL from 1995 to Present

This script generates comprehensive training data using daily OHLCV data
with technical indicators and price prediction targets.
"""

import asyncio
import asyncpg
import logging
import pandas as pd
import numpy as np
import os
from datetime import datetime, date, timedelta
from pathlib import Path
import json
import pickle
# Removed talib dependency - using pandas-based calculations

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def generate_daily_training_data(symbol='AAPL'):
    """Generate comprehensive daily training data for a given symbol."""
    
    logger.info("="*80)
    logger.info(f"{symbol} DAILY TRAINING DATA GENERATION")
    logger.info(f"Period: Historical data to Present")
    logger.info("="*80)
    
    try:
        # Connect to database using environment variables
        environment = os.environ.get('ENVIRONMENT', 'dev')
        
        # Use appropriate database connection based on environment
        if environment == 'intg':
            # Integration environment uses host networking to access localhost:4432
            db_host = 'localhost'  # Host networking allows direct localhost access
            db_port = 4432  # Integration PostgreSQL on port 4432
            db_user = 'postgres'
            db_password = 'intg_password'
            db_name = 'intg_db'
        else:
            # Development environment - use container networking
            db_host = 'ats-dev-postgres'  # Container name on ats-network
            db_port = 5432  # Internal container port
            db_user = 'postgres'
            db_password = 'dev_password'  # ATS-DEV password
            db_name = 'dev_db'
        
        logger.info(f"🔌 Connecting to {db_host}:{db_port}/{db_name} ({environment} environment)")
        
        conn = await asyncpg.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name
        )
        
        # Get data from database - try multiple sources
        logger.info(f"📊 Fetching {symbol} daily price data...")
        
        # Try EODHD first (more complete data for TSLA)
        symbol_data = await conn.fetch(f"""
            SELECT 
                e.date as date,
                e.open as open,
                e.high as high,
                e.low as low,
                e.close as close,
                e.adjusted_close as adjclose,
                e.volume as volume,
                'EODHD' as source
            FROM {environment}_instruments i
            JOIN {environment}_daily_prices_eodhd e ON i.id = e.instrument_id
            WHERE i.symbol = '{symbol}'
            ORDER BY e.date
        """)
        
        # If no EODHD data, try Tiingo
        if not symbol_data:
            symbol_data = await conn.fetch(f"""
                SELECT 
                    t.date as date,
                    t.open as open,
                    t.high as high,
                    t.low as low,
                    t.close as close,
                    t.adjclose as adjclose,
                    t.volume as volume,
                    'Tiingo' as source
                FROM {environment}_instruments i
                JOIN {environment}_daily_prices_tiingo t ON i.id = t.instrument_id
                WHERE i.symbol = '{symbol}'
                ORDER BY t.date
            """)
            
        # If still no data, try Polygon
        if not symbol_data:
            symbol_data = await conn.fetch(f"""
                SELECT 
                    p.date as date,
                    p.open as open,
                    p.high as high,
                    p.low as low,
                    p.close as close,
                    p.close as adjclose,  -- Use close as adjusted close for Polygon
                    p.volume as volume,
                    'Polygon' as source
                FROM {environment}_instruments i
                JOIN {environment}_daily_prices_polygon p ON i.id = p.instrument_id
                WHERE i.symbol = '{symbol}'
                ORDER BY p.date
            """)
        
        await conn.close()
        
        if not symbol_data:
            logger.error(f"❌ No {symbol} data found in database")
            return
        
        # Convert asyncpg Records to pandas DataFrame
        df_data = []
        data_source = None
        for record in symbol_data:
            record_dict = dict(record)
            if data_source is None:
                data_source = record_dict.get('source', 'Unknown')
            df_data.append(record_dict)
        
        df = pd.DataFrame(df_data)
        logger.info(f"📋 Available columns: {df.columns.tolist()}")
        
        # Convert date column to datetime and set as index
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date').sort_index()
        
        # Convert numeric columns to float (in case they come as Decimal from PostgreSQL)
        numeric_cols = ['open', 'high', 'low', 'close', 'adjclose', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        logger.info(f"✅ Loaded {len(df):,} days of {symbol} data from {data_source}")
        logger.info(f"📅 Date range: {df.index[0].date()} to {df.index[-1].date()}")
        
        # Generate technical indicators
        logger.info("🔧 Generating technical indicators...")
        
        # Price-based indicators
        df['sma_5'] = df['close'].rolling(5).mean()
        df['sma_10'] = df['close'].rolling(10).mean()
        df['sma_20'] = df['close'].rolling(20).mean()
        df['sma_50'] = df['close'].rolling(50).mean()
        df['sma_200'] = df['close'].rolling(200).mean()
        
        df['ema_12'] = df['close'].ewm(span=12).mean()
        df['ema_26'] = df['close'].ewm(span=26).mean()
        
        # MACD
        df['macd'] = df['ema_12'] - df['ema_26']
        df['macd_signal'] = df['macd'].ewm(span=9).mean()
        df['macd_histogram'] = df['macd'] - df['macd_signal']
        
        # RSI
        df['rsi'] = calculate_rsi(df['close'], 14)
        
        # Bollinger Bands
        bb_period = 20
        bb_std = 2
        df['bb_middle'] = df['close'].rolling(bb_period).mean()
        bb_std_dev = df['close'].rolling(bb_period).std()
        df['bb_upper'] = df['bb_middle'] + (bb_std_dev * bb_std)
        df['bb_lower'] = df['bb_middle'] - (bb_std_dev * bb_std)
        df['bb_width'] = df['bb_upper'] - df['bb_lower']
        df['bb_position'] = (df['close'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'])
        
        # Volume indicators
        df['volume_sma_20'] = df['volume'].rolling(20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_sma_20']
        
        # Price change indicators
        df['price_change_1d'] = df['close'].pct_change(1)
        df['price_change_5d'] = df['close'].pct_change(5)
        df['price_change_20d'] = df['close'].pct_change(20)
        
        # Volatility
        df['volatility_20d'] = df['price_change_1d'].rolling(20).std()
        df['high_low_pct'] = (df['high'] - df['low']) / df['close']
        
        # Support and Resistance levels (simplified)
        df['support_20d'] = df['low'].rolling(20).min()
        df['resistance_20d'] = df['high'].rolling(20).max()
        df['support_distance'] = (df['close'] - df['support_20d']) / df['close']
        df['resistance_distance'] = (df['resistance_20d'] - df['close']) / df['close']
        
        # Trend indicators
        df['trend_5d'] = np.where(df['close'] > df['sma_5'], 1, -1)
        df['trend_20d'] = np.where(df['close'] > df['sma_20'], 1, -1)
        df['trend_50d'] = np.where(df['close'] > df['sma_50'], 1, -1)
        
        # Generate prediction targets
        logger.info("🎯 Generating prediction targets...")
        
        # Next day price movement (classification target)
        df['next_day_return'] = df['price_change_1d'].shift(-1)
        df['next_day_up'] = (df['next_day_return'] > 0).astype(int)
        df['next_day_big_move'] = (abs(df['next_day_return']) > 0.02).astype(int)  # >2% move
        
        # Multi-day targets
        df['next_5day_return'] = df['close'].pct_change(5).shift(-5)
        df['next_5day_up'] = (df['next_5day_return'] > 0).astype(int)
        df['next_5day_max'] = df['high'].rolling(5).max().shift(-5)
        df['next_5day_min'] = df['low'].rolling(5).min().shift(-5)
        
        # Risk-adjusted targets
        df['next_day_sharpe'] = df['next_day_return'] / df['volatility_20d']
        
        # Define feature columns before cleaning
        base_features = [
            'open', 'high', 'low', 'close', 'volume',
            'sma_5', 'sma_10', 'sma_20', 'sma_50', 'sma_200',
            'ema_12', 'ema_26', 'macd', 'macd_signal', 'macd_histogram',
            'rsi', 'bb_middle', 'bb_upper', 'bb_lower', 'bb_width', 'bb_position',
            'volume_sma_20', 'volume_ratio',
            'price_change_1d', 'price_change_5d', 'price_change_20d',
            'volatility_20d', 'high_low_pct',
            'support_20d', 'resistance_20d', 'support_distance', 'resistance_distance',
            'trend_5d', 'trend_20d', 'trend_50d'
        ]
        
        # Add adjclose only if it has valid data
        feature_cols = base_features.copy()
        if not df['adjclose'].isna().all():
            feature_cols.append('adjclose')
        
        # Debug: check for NaN values before cleaning
        nan_cols = df.columns[df.isna().any()].tolist()
        if nan_cols:
            logger.warning(f"⚠️ Columns with NaN values: {nan_cols}")
            for col in nan_cols[:5]:  # Show first 5 columns with NaN
                nan_count = df[col].isna().sum()
                logger.info(f"  {col}: {nan_count} NaN values")
        
        # Clean data more selectively - only require core price data
        # Drop rows with NaN in essential columns, but allow NaN in derived features for now
        essential_cols = ['open', 'high', 'low', 'close', 'volume']
        df_clean = df.dropna(subset=essential_cols)
        
        # Then drop any remaining rows with too many NaN values in features/targets
        # Keep rows that have at least 80% of feature data and valid targets
        required_feature_threshold = 0.8
        feature_na_counts = df_clean[base_features].isna().sum(axis=1)
        max_na_allowed = len(base_features) * (1 - required_feature_threshold)
        df_clean = df_clean[feature_na_counts <= max_na_allowed]
        
        # Drop rows with NaN in prediction targets
        df_clean = df_clean.dropna(subset=['next_day_return', 'next_day_up'])
        
        logger.info(f"📈 Training dataset: {len(df_clean):,} samples after cleaning (was {len(df):,})")
        
        target_cols = [
            'next_day_return', 'next_day_up', 'next_day_big_move',
            'next_5day_return', 'next_5day_up', 'next_day_sharpe'
        ]
        
        # Create training datasets for different time periods
        # Determine start dates based on available data
        first_date = df.index[0].strftime('%Y-%m-%d')
        symbol_lower = symbol.lower()
        
        datasets = {
            f'{symbol_lower}_full_history': {
                'start_date': first_date,
                'end_date': '2025-08-29',
                'description': f'{symbol} complete historical training data'
            },
            f'{symbol_lower}_last_10_years': {
                'start_date': '2015-01-01',
                'end_date': '2025-08-29',
                'description': f'{symbol} last 10 years training data'
            },
            f'{symbol_lower}_last_5_years': {
                'start_date': '2020-01-01',
                'end_date': '2025-08-29',
                'description': f'{symbol} last 5 years training data'
            }
        }
        
        results = {}
        
        for dataset_name, config in datasets.items():
            logger.info(f"📊 Creating {dataset_name} dataset...")
            
            # Filter data by date range
            mask = (df_clean.index >= config['start_date']) & (df_clean.index <= config['end_date'])
            dataset_df = df_clean[mask].copy()
            
            if len(dataset_df) == 0:
                logger.warning(f"❌ No data for {dataset_name}")
                continue
            
            # Create features and targets
            X = dataset_df[feature_cols]
            y = dataset_df[target_cols]
            
            # Create training examples
            training_examples = []
            for idx, (date, row) in enumerate(dataset_df.iterrows()):
                example = {
                    'date': date.strftime('%Y-%m-%d'),
                    'symbol': symbol,
                    'features': row[feature_cols].to_dict(),
                    'targets': row[target_cols].to_dict(),
                    'raw_prices': {
                        'open': float(row['open']),
                        'high': float(row['high']),
                        'low': float(row['low']),
                        'close': float(row['close']),
                        'volume': int(row['volume'])
                    }
                }
                training_examples.append(example)
            
            # Save training data
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_dir = Path(f"/data/training/{dataset_name}")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Save pickle file
            pickle_file = output_dir / f"{dataset_name}_{timestamp}.pkl"
            with open(pickle_file, 'wb') as f:
                pickle.dump(training_examples, f)
            
            # Save CSV file
            csv_file = output_dir / f"{dataset_name}_{timestamp}.csv"
            dataset_df.to_csv(csv_file)
            
            # Generate summary
            summary = {
                'dataset_name': dataset_name,
                'generation_timestamp': datetime.now().isoformat(),
                'total_examples': len(training_examples),
                'feature_count': len(feature_cols),
                'target_count': len(target_cols),
                'date_range': {
                    'start': dataset_df.index[0].strftime('%Y-%m-%d'),
                    'end': dataset_df.index[-1].strftime('%Y-%m-%d'),
                    'days': len(dataset_df)
                },
                'files': {
                    'pickle': str(pickle_file),
                    'csv': str(csv_file)
                },
                'feature_columns': feature_cols,
                'target_columns': target_cols,
                'statistics': {
                    'avg_daily_return': float(dataset_df['price_change_1d'].mean()),
                    'volatility': float(dataset_df['price_change_1d'].std()),
                    'price_range': {
                        'min': float(dataset_df['close'].min()),
                        'max': float(dataset_df['close'].max()),
                        'latest': float(dataset_df['close'].iloc[-1])
                    },
                    'volume_stats': {
                        'avg': float(dataset_df['volume'].mean()),
                        'max': int(dataset_df['volume'].max())
                    }
                }
            }
            
            # Save summary
            summary_file = output_dir / f"{dataset_name}_summary_{timestamp}.json"
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            results[dataset_name] = summary
            
            logger.info(f"✅ {dataset_name}: {len(training_examples):,} examples")
            logger.info(f"📈 Price range: ${summary['statistics']['price_range']['min']:.2f} - ${summary['statistics']['price_range']['max']:.2f}")
            logger.info(f"📁 Files saved to: {output_dir}")
        
        # Save overall summary
        overall_summary = {
            'generation_timestamp': datetime.now().isoformat(),
            'symbol': symbol,
            'data_source': data_source,
            'total_datasets': len(results),
            'successful_generations': len(results),
            'datasets': results
        }
        
        summary_file = Path(f"/data/training/{symbol_lower}_daily_comprehensive_summary.json")
        summary_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(summary_file, 'w') as f:
            json.dump(overall_summary, f, indent=2, default=str)
        
        logger.info("\n" + "="*80)
        logger.info(f"{symbol} DAILY TRAINING DATA GENERATION COMPLETED")
        logger.info("="*80)
        logger.info(f"Successfully generated: {len(results)} datasets")
        logger.info(f"Overall summary saved to: {summary_file}")
        
        for name, result in results.items():
            logger.info(f"✅ {name}: {result['total_examples']:,} examples, {result['feature_count']} features")
        
        return overall_summary
        
    except Exception as e:
        logger.error(f"❌ Error generating {symbol} training data: {e}")
        import traceback
        traceback.print_exc()
        raise

def calculate_rsi(prices, period=14):
    """Calculate RSI indicator"""
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

if __name__ == "__main__":
    import sys
    symbol = sys.argv[1] if len(sys.argv) > 1 else 'AAPL'
    asyncio.run(generate_daily_training_data(symbol))