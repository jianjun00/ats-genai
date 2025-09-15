# 🚨 WARNING: THIS SCRIPT GENERATES SYNTHETIC DATA - FOR TESTING ONLY
#!/usr/bin/env python3
"""
Simple AAPL 2025 training data generation using run_dev infrastructure
"""

import asyncio
import logging
from datetime import date, datetime
from pathlib import Path
import json
import pandas as pd
import asyncpg
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def generate_aapl_2025_data():
    """Generate AAPL 2025 training data using INTG database."""

    logger.info("🚀 Starting AAPL 2025 training data generation")

    # Configuration
    symbol = 'AAPL'
    start_date = date(2025, 1, 1)
    end_date = date(2025, 9, 1)

    # Database connection - use INTG environment
    try:
        connection = await asyncpg.connect(
            host='localhost',
            port=4432,
            user='postgres',
            password='intg_password',
            database='intg_db'
        )

        logger.info(f"📊 Querying data for {symbol} from {start_date} to {end_date}")

        # Query daily price data from INTG Tiingo table (has 2025 data)
        query = """
        SELECT date, open, high, low, close, volume
        FROM intg_daily_price_tiingo
        WHERE symbol = $1 AND date >= $2 AND date <= $3
        ORDER BY date
        """

        rows = await connection.fetch(query, symbol, start_date, end_date)

        if not rows:
            logger.error(f"❌ No data found for {symbol} in INTG database")
            return

        logger.info(f"📈 Found {len(rows)} daily price records")

        # Convert to DataFrame with proper column names
        column_names = ['date', 'open', 'high', 'low', 'close', 'volume']
        df = pd.DataFrame([dict(row) for row in rows])
        logger.info(f"📊 DataFrame columns: {list(df.columns)}")
        logger.info(f"📊 DataFrame shape: {df.shape}")

        df['symbol'] = symbol
        df['datetime'] = pd.to_datetime(df['date'])

        # Add technical indicators
        logger.info("🔧 Computing technical indicators...")
        df['sma_20'] = df['close'].rolling(window=20, min_periods=1).mean()
        df['ema_12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema_26'] = df['close'].ewm(span=26, adjust=False).mean()

        # Envelope indicators
        df['etop'] = df['close'] * 1.05
        df['ebot'] = df['close'] * 0.95
        df['pldot'] = df['close']

        # Time features
        df['timestamp'] = df['datetime'].astype('int64') // 10**9
        df['year'] = df['datetime'].dt.year
        df['month'] = df['datetime'].dt.month
        df['day'] = df['datetime'].dt.day
        df['hour'] = 16  # Market close
        df['weekday'] = df['datetime'].dt.weekday

        # Multi-timeframe (using daily as proxy)
        for tf in ['5m', '15m', '1h']:
            df[f'{tf}_high'] = df['high']
            df[f'{tf}_low'] = df['low']
            df[f'{tf}_close'] = df['close']

        # Returns and volatility
        df['return_1d'] = df['close'].pct_change()
        df['return_5d'] = df['close'].pct_change(periods=5)
        df['return_20d'] = df['close'].pct_change(periods=20)
        df['volatility_20d'] = df['return_1d'].rolling(window=20, min_periods=1).std()
        df['volume_sma_20'] = df['volume'].rolling(window=20, min_periods=1).mean()
        df['volume_ratio'] = df['volume'] / df['volume_sma_20']

        # Fill NaN values
        df = df.fillna(method='ffill').fillna(method='bfill')

        # Create output directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dataset_name = f"aapl_2025_training_{timestamp}"
        output_dir = Path("/mnt/d/ats-data/training") / dataset_name
        output_dir.mkdir(parents=True, exist_ok=True)

        # Column order for CSV
        column_order = [
            'datetime', 'symbol', 'timestamp', 'year', 'month', 'day', 'hour', 'weekday',
            'open', 'high', 'low', 'close', 'volume',
            'sma_20', 'ema_12', 'ema_26', 'etop', 'ebot', 'pldot',
            '5m_high', '5m_low', '5m_close',
            '15m_high', '15m_low', '15m_close',
            '1h_high', '1h_low', '1h_close',
            'return_1d', 'return_5d', 'return_20d',
            'volatility_20d', 'volume_sma_20', 'volume_ratio'
        ]

        df_ordered = df[column_order]

        # Save CSV
        csv_path = output_dir / f"{dataset_name}.csv"
        df_ordered.to_csv(csv_path, index=False)
        logger.info(f"💾 Saved CSV: {csv_path}")

        # Create metadata
        metadata = {
            "symbol": symbol,
            "num_rows": len(df_ordered),
            "num_features": len(df_ordered.columns),
            "feature_names": list(df_ordered.columns),
            "date_range": [start_date.isoformat(), end_date.isoformat()],
            "datetime_features": ['datetime', 'timestamp', 'year', 'month', 'day', 'hour', 'weekday'],
            "technical_indicators": ['sma_20', 'ema_12', 'ema_26', 'etop', 'ebot', 'pldot'],
            "data_format": "csv_time_series",
            "sequence_length": 1,
            "time_resolution": "daily",
            "visualization_type": "time_series",
            "time_step_unit": "day",
            "is_time_series": True,
            "window_size": 21,
            "generation_timestamp": datetime.now().isoformat(),
            "csv_file": str(csv_path)
        }

        # Save metadata
        metadata_path = output_dir / f"{dataset_name}_metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"📋 Saved metadata: {metadata_path}")

        # Insert into database
        file_size_mb = csv_path.stat().st_size / (1024 * 1024)
        feature_metadata_json = {
            "ohlc_columns": {
                "open": "open", "high": "high", "low": "low",
                "close": "close", "volume": "volume"
            },
            "technical_indicators": metadata["technical_indicators"],
            "datetime_column": "datetime"
        }

        insert_query = """
        INSERT INTO intg_training_dataset (
            dataset_name, total_sequences, feature_count, label_count,
            data_quality_score, feature_completeness, label_completeness,
            file_size_mb, technical_indicators, symbols,
            date_range_start, date_range_end,
            data_format, sequence_length, time_resolution,
            visualization_type, time_step_unit, is_time_series,
            window_size, feature_metadata
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
        ) RETURNING id
        """

        dataset_id = await connection.fetchval(
            insert_query,
            dataset_name,           # dataset_name
            len(df_ordered),        # total_sequences
            len(df_ordered.columns), # feature_count
            0,                      # label_count
            1.0,                    # data_quality_score
            1.0,                    # feature_completeness
            1.0,                    # label_completeness
            file_size_mb,           # file_size_mb
            ','.join(metadata["technical_indicators"]), # technical_indicators
            json.dumps([symbol]),   # symbols
            start_date,             # date_range_start
            end_date,               # date_range_end
            'csv_time_series',      # data_format
            1,                      # sequence_length
            'daily',                # time_resolution
            'time_series',          # visualization_type
            'day',                  # time_step_unit
            True,                   # is_time_series
            21,                     # window_size
            json.dumps(feature_metadata_json)  # feature_metadata
        )

        # Summary
        logger.info("🎉 AAPL 2025 training data generation completed!")
        logger.info(f"📊 Dataset: {dataset_name}")
        logger.info(f"📈 Records: {len(df_ordered):,}")
        logger.info(f"🗓️ Date range: {start_date} to {end_date}")
        logger.info(f"💾 Size: {file_size_mb:.2f} MB")
        logger.info(f"🆔 Database ID: {dataset_id}")
        logger.info(f"📁 Location: {output_dir}")

        return dataset_id

    except Exception as e:
        logger.error(f"❌ Error: {e}")
        raise
    finally:
        if 'connection' in locals():
            await connection.close()

if __name__ == "__main__":
    asyncio.run(generate_aapl_2025_data())