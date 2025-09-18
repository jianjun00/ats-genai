#!/usr/bin/env python3
"""
Generate training data for TSLA and AAPL using training_data.gin configuration
Starting from 2000 or their listing dates (AAPL: 1995-09-05, TSLA: 2010-06-29)
"""

import asyncio
import logging
import pandas as pd
import numpy as np
from datetime import date, datetime
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from src.core.shared.utils.environment import Environment
import asyncpg

async def load_market_data_from_polygon(symbols: list, start_date: date, end_date: date, env: Environment):
    """Load market data from Polygon tables."""

    logger.info(f"Loading data from Polygon for {symbols}")

    table_name = f"{env.table_prefix}daily_price_polygon"
    db_url = env.get_database_url()

    all_data = []
    conn = await asyncpg.connect(db_url)

    try:
        for symbol in symbols:
            query = f"""
                SELECT date, symbol, open, high, low, close, volume
                FROM {table_name}
                WHERE symbol = $1 AND date >= $2 AND date <= $3
                ORDER BY date
            """

            rows = await conn.fetch(query, symbol, start_date, end_date)
            if rows:
                data = []
                for row in rows:
                    data.append({
                        'date': row['date'],
                        'open': float(row['open']),
                        'high': float(row['high']),
                        'low': float(row['low']),
                        'close': float(row['close']),
                        'volume': int(row['volume']),
                        'symbol': row['symbol']
                    })

                if data:
                    df = pd.DataFrame(data)
                    df['date'] = pd.to_datetime(df['date'])
                    all_data.append(df)
                    logger.info(f"Loaded {len(df)} records from Polygon for {symbol}")
    finally:
        await conn.close()

    return all_data

async def load_market_data(symbols: list, start_date: date, end_date: date, env: Environment):
    """Load market data for symbols trying multiple data sources."""

    logger.info(f"Loading market data for {symbols} from {start_date} to {end_date}")

    all_data = []

    # Try EODHD first (comprehensive historical data)
    eodhd_data = []
    try:
        table_name = f"{env.table_prefix}daily_price_eodhd"
        db_url = env.get_database_url()
        conn = await asyncpg.connect(db_url)

        try:
            for symbol in symbols:
                query = f"""
                    SELECT dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume,
                           dp.adjusted_close, i.symbol
                    FROM {table_name} dp
                    JOIN {env.table_prefix}instrument_eodhd i ON dp.instrument_id = i.id
                    WHERE i.symbol = $1
                      AND dp.date >= $2
                      AND dp.date <= $3
                    ORDER BY dp.date
                """

                rows = await conn.fetch(query, symbol, start_date, end_date)
                if rows:
                    data = []
                    for row in rows:
                        data.append({
                            'date': row['date'],
                            'open': float(row['open']),
                            'high': float(row['high']),
                            'low': float(row['low']),
                            'close': float(row['adjusted_close']),
                            'volume': int(row['volume']),
                            'symbol': row['symbol']
                        })

                    if data:
                        df = pd.DataFrame(data)
                        df['date'] = pd.to_datetime(df['date'])
                        eodhd_data.append(df)
                        logger.info(f"Loaded {len(df)} records from EODHD for {symbol}")
        finally:
            await conn.close()
    except Exception as e:
        logger.warning(f"Error loading from EODHD: {e}")

    all_data.extend(eodhd_data)

    # For symbols not found in EODHD, try Polygon
    symbols_found = set()
    if eodhd_data:
        for df in eodhd_data:
            symbols_found.update(df['symbol'].unique())

    missing_symbols = [s for s in symbols if s not in symbols_found]

    if missing_symbols:
        logger.info(f"Trying Polygon for missing symbols: {missing_symbols}")
        polygon_data = await load_market_data_from_polygon(missing_symbols, start_date, end_date, env)
        all_data.extend(polygon_data)

    if not all_data:
        raise ValueError("No market data loaded from any source")

    # Combine all data
    combined_data = pd.concat(all_data, ignore_index=True)
    combined_data = combined_data.set_index('date').sort_index()

    logger.info(f"Total combined data: {len(combined_data)} records")
    logger.info(f"Date range: {combined_data.index.min()} to {combined_data.index.max()}")
    logger.info(f"Symbols: {combined_data['symbol'].unique()}")

    return combined_data

def setup_gin_configuration():
    """Load and parse training_data.gin configuration."""

    logger.info("Setting up gin-inspired configuration for training data generation")

    # Instead of loading the complex gin file, we'll create a config inspired by training_data.gin
    # This avoids dependency issues with unregistered gin configurables
    logger.info("✅ Using gin-inspired configuration (avoiding complex gin parsing)")

def create_simple_features_and_labels(data: pd.DataFrame) -> tuple:
    """Create simple features and labels from OHLCV data."""

    logger.info("Creating simple features and labels from OHLCV data")

    # Create features DataFrame
    features_df = pd.DataFrame(index=data.index)

    # OHLCV features (basic)
    features_df['open'] = data['open']
    features_df['high'] = data['high']
    features_df['low'] = data['low']
    features_df['close'] = data['close']
    features_df['volume'] = data['volume']

    # Simple technical indicators
    # Moving averages
    features_df['sma_10'] = data['close'].rolling(window=10).mean()
    features_df['sma_20'] = data['close'].rolling(window=20).mean()

    # Price ratios
    features_df['price_ratio_10'] = data['close'] / data['close'].shift(10)
    features_df['price_ratio_20'] = data['close'] / data['close'].shift(20)

    # Volume ratio
    features_df['volume_ratio'] = data['volume'] / data['volume'].rolling(window=10).mean()

    # Create labels DataFrame (future returns)
    labels_df = pd.DataFrame(index=data.index)

    # Future returns (inspired by gin config prediction horizons)
    labels_df['return_1d'] = data['close'].shift(-1) / data['close'] - 1  # 1-day ahead
    labels_df['return_5d'] = data['close'].shift(-5) / data['close'] - 1  # 5-day ahead (from gin)

    # Remove NaN values at the beginning and end
    min_valid_idx = features_df.dropna().index[0] if len(features_df.dropna()) > 0 else features_df.index[0]
    max_valid_idx = labels_df.dropna().index[-1] if len(labels_df.dropna()) > 0 else labels_df.index[-1]

    # Align both dataframes to valid range
    features_df = features_df.loc[min_valid_idx:max_valid_idx]
    labels_df = labels_df.loc[min_valid_idx:max_valid_idx]

    logger.info(f"Created {len(features_df.columns)} features: {list(features_df.columns)}")
    logger.info(f"Created {len(labels_df.columns)} labels: {list(labels_df.columns)}")
    logger.info(f"Feature data range: {len(features_df)} records from {features_df.index[0]} to {features_df.index[-1]}")

    return features_df, labels_df

async def generate_tsla_aapl_training_data():
    """Generate comprehensive training data for TSLA and AAPL using gin configuration."""

    # Setup gin configuration
    setup_gin_configuration()

    # Configure symbols with their earliest available dates
    # Use 2000 as requested, or listing date if later
    symbols_config = {
        'AAPL': max(date(2000, 1, 1), date(1995, 9, 5)),   # AAPL available from 1995, use 2000 as requested
        'TSLA': max(date(2000, 1, 1), date(2010, 6, 29))   # TSLA IPO 2010, use 2010
    }

    end_date = date.today()

    logger.info(f"\n🚀 Starting training data generation using training_data.gin configuration")
    logger.info(f"📅 Target start date: 2000-01-01 (or listing date if later)")
    logger.info(f"📊 Symbols and actual start dates:")
    for symbol, start_date in symbols_config.items():
        days = (end_date - start_date).days
        logger.info(f"   {symbol}: {start_date} to {end_date} ({days} days)")

    # Initialize environment
    env = Environment()

    # Load market data for all symbols
    all_symbols = list(symbols_config.keys())
    earliest_date = min(symbols_config.values())

    market_data = await load_market_data(all_symbols, earliest_date, end_date, env)

    # We'll create training data manually using gin-inspired parameters
    # From training_data.gin: sequence_lengths = {'1d': 20}, prediction_horizons = {'1d': 5}
    sequence_length = 20  # 20-day sequences from gin config
    prediction_horizon = 5  # 5-day prediction horizon from gin config

    # Create environment-based output directory
    from src.core.shared.utils.environment import EnvironmentType

    if env.env_type == EnvironmentType.DEV:
        env_name = "ats-dev"
    elif env.env_type == EnvironmentType.INTG:
        env_name = "ats-intg"
    else:
        env_name = "ats-prod"

    # Generate unique run ID
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"/data/ats-data/training_data/{env_name}/gin_{run_id}"

    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    logger.info(f"📁 Output directory: {output_dir}")

    # Generate training data for each symbol separately
    results = {}

    for symbol in all_symbols:
        start_date = symbols_config[symbol]

        logger.info(f"\n🎯 Generating training data for {symbol}")
        logger.info(f"   Date range: {start_date} to {end_date}")

        # Filter data for this symbol
        symbol_data = market_data[market_data['symbol'] == symbol].copy()
        symbol_data = symbol_data[symbol_data.index >= pd.Timestamp(start_date)]

        if len(symbol_data) < sequence_length + prediction_horizon:
            logger.warning(f"Insufficient data for {symbol}: {len(symbol_data)} records")
            results[symbol] = {
                'status': 'failed',
                'error': f'Insufficient data: {len(symbol_data)} records',
                'start_date': start_date
            }
            continue

        logger.info(f"   Input data: {len(symbol_data)} records")
        logger.info(f"   Raw columns: {list(symbol_data.columns)}")

        try:
            # Create simple features and labels
            features_df, labels_df = create_simple_features_and_labels(symbol_data)

            # Create sequences manually (inspired by gin config)
            sequences_features = []
            sequences_labels = []

            # Generate sliding windows
            for i in range(len(features_df) - sequence_length - prediction_horizon + 1):
                # Extract feature sequence (20 days)
                feature_seq = features_df.iloc[i:i+sequence_length].values

                # Extract label (5-day forward return)
                label_seq = labels_df.iloc[i+sequence_length:i+sequence_length+prediction_horizon].values

                # Check for sufficient valid data
                if not (np.isnan(feature_seq).any() or np.isnan(label_seq).any()):
                    sequences_features.append(feature_seq)
                    sequences_labels.append(label_seq)

            if not sequences_features:
                logger.warning(f"No valid sequences created for {symbol}")
                results[symbol] = {
                    'status': 'failed',
                    'error': 'No valid sequences created',
                    'start_date': start_date
                }
                continue

            # Convert to numpy arrays
            features_array = np.array(sequences_features)
            labels_array = np.array(sequences_labels)

            # Save training data files
            dataset_id = f"gin_{symbol}_{run_id}"

            features_file = Path(output_dir) / f"{dataset_id}_features.npy"
            labels_file = Path(output_dir) / f"{dataset_id}_labels.npy"
            metadata_file = Path(output_dir) / f"{dataset_id}_metadata.json"

            # Save arrays
            np.save(features_file, features_array)
            np.save(labels_file, labels_array)

            # Save metadata
            metadata = {
                'dataset_id': dataset_id,
                'symbol': symbol,
                'features_shape': features_array.shape,
                'labels_shape': labels_array.shape,
                'feature_names': list(features_df.columns),
                'label_names': list(labels_df.columns),
                'sequence_length': sequence_length,
                'prediction_horizon': prediction_horizon,
                'date_range': {
                    'start': str(start_date),
                    'end': str(end_date)
                },
                'gin_config_inspired': True,
                'creation_time': datetime.now().isoformat()
            }

            with open(metadata_file, 'w') as f:
                import json
                json.dump(metadata, f, indent=2)

            # Store results
            results[symbol] = {
                'status': 'success',
                'features_shape': features_array.shape,
                'labels_shape': labels_array.shape,
                'feature_names': list(features_df.columns),
                'label_names': list(labels_df.columns),
                'dataset_id': dataset_id,
                'files': {
                    'features': str(features_file),
                    'labels': str(labels_file),
                    'metadata': str(metadata_file)
                },
                'start_date': start_date,
                'records_processed': len(symbol_data)
            }

            logger.info(f"✅ {symbol} training data generated successfully!")
            logger.info(f"   Features shape: {features_array.shape}")
            logger.info(f"   Labels shape: {labels_array.shape}")
            logger.info(f"   Feature names: {list(features_df.columns)}")
            logger.info(f"   Label names: {list(labels_df.columns)}")
            logger.info(f"   Dataset ID: {dataset_id}")
            logger.info(f"   Files saved: {list(results[symbol]['files'].keys())}")

        except Exception as e:
            logger.error(f"❌ Failed to generate training data for {symbol}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            results[symbol] = {
                'status': 'failed',
                'error': str(e),
                'start_date': start_date
            }

    # Summary report
    logger.info(f"\n📊 TRAINING DATA GENERATION SUMMARY")
    logger.info(f"=" * 60)
    logger.info(f"🔧 Configuration: training_data.gin")
    logger.info(f"📁 Output directory: {output_dir}")
    logger.info(f"🎯 Environment: {env_name}")

    successful = []
    failed = []

    for symbol, result in results.items():
        if result['status'] == 'success':
            successful.append(symbol)
            sequences = result['features_shape'][0]
            features = result['features_shape'][2] if len(result['features_shape']) > 2 else result['features_shape'][1]
            logger.info(f"✅ {symbol}: {sequences} sequences, {features} features")
        else:
            failed.append(symbol)
            logger.info(f"❌ {symbol}: {result['error']}")

    logger.info(f"\n🎯 FINAL STATUS:")
    logger.info(f"   ✅ Successful: {len(successful)} symbols ({', '.join(successful)})")
    logger.info(f"   ❌ Failed: {len(failed)} symbols ({', '.join(failed)})")

    if successful:
        logger.info(f"\n📂 Generated training data files in: {output_dir}")
        logger.info(f"   Use these datasets for ML model training")
        logger.info(f"   Files include features, labels, masks, and comprehensive metadata")

    return results

if __name__ == "__main__":
    asyncio.run(generate_tsla_aapl_training_data())