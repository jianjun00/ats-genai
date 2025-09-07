# 🚨 WARNING: THIS SCRIPT GENERATES SYNTHETIC DATA - FOR TESTING ONLY
#!/usr/bin/env python3
"""
Generate new training data specifically for visualization with ArrayRecord format.
Creates OHLC data that can be displayed in Plotly charts and table views.
"""
import os
import sys
import json
import asyncio
import asyncpg
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from array_record.python.array_record_module import ArrayRecordWriter

def create_sample_ohlc_data(symbol: str, num_bars: int = 21):
    """Create sample OHLC data for visualization."""
    np.random.seed(42 if symbol == 'AAPL' else 123)  # Deterministic but different per symbol

    # Starting price based on symbol
    base_price = 180.0 if symbol == 'AAPL' else 250.0

    data = []
    current_price = base_price

    for i in range(num_bars):
        # Generate realistic OHLC data
        open_price = current_price + np.random.normal(0, 1.0)

        # High and low relative to open
        high_offset = abs(np.random.normal(2.0, 1.0))
        low_offset = -abs(np.random.normal(2.0, 1.0))

        high_price = open_price + high_offset
        low_price = open_price + low_offset

        # Close price between high and low
        close_price = low_price + (high_price - low_price) * np.random.beta(2, 2)
        current_price = close_price

        # Volume
        volume = int(np.random.normal(1000000, 200000))
        volume = max(100000, volume)  # Minimum volume

        # Technical indicators (envelope bands)
        envelope_top = high_price * 1.02  # 2% above high
        envelope_bot = low_price * 0.98   # 2% below low
        pldot = (high_price + low_price) / 2  # Mid-point

        # Create timestamp
        timestamp = datetime(2025, 8, 1) + timedelta(hours=i)

        bar_data = {
            "time_step": i,
            "datetime": timestamp.isoformat(),
            "symbol": symbol,
            "open": round(open_price, 2),
            "high": round(high_price, 2),
            "low": round(low_price, 2),
            "close": round(close_price, 2),
            "volume": volume,
            "envelope_top": round(envelope_top, 2),
            "envelope_bot": round(envelope_bot, 2),
            "pldot": round(pldot, 2),
            # Additional features for ML
            "hour_of_day": timestamp.hour,
            "day_of_week": timestamp.weekday(),
            "rsi": round(50 + np.random.normal(0, 15), 2),  # RSI indicator
            "macd": round(np.random.normal(0, 2), 4),        # MACD indicator
        }

        data.append(bar_data)

    return data

async def create_training_dataset_record(symbol: str, data: list) -> int:
    """Create database record for the training dataset."""
    connection_str = "postgresql://postgres:dev_password@localhost:3432/dev_db"

    conn = await asyncpg.connect(connection_str)
    try:
        # Insert dataset record
        insert_query = """
        INSERT INTO dev_training_datasets (
            dataset_name, run_id, total_sequences, sequence_length, feature_count,
            label_count, symbols, date_range_start, date_range_end, data_quality_score,
            feature_completeness, label_completeness, file_size_mb, data_sources,
            status, technical_indicators, created_by, data_format, time_resolution,
            visualization_type, is_time_series, window_size
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
        ) RETURNING id
        """

        dataset_name = f"Visualization_Ready_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        run_id = int(datetime.now().timestamp())

        dataset_id = await conn.fetchval(
            insert_query,
            dataset_name,                           # dataset_name
            run_id,                                # run_id
            len(data),                             # total_sequences
            len(data),                             # sequence_length
            len(data[0].keys()) if data else 0,    # feature_count
            0,                                     # label_count
            [symbol],                              # symbols (array)
            "2025-08-01",                          # date_range_start
            "2025-08-01",                          # date_range_end
            1.0,                                   # data_quality_score
            1.0,                                   # feature_completeness
            1.0,                                   # label_completeness
            0.0,                                   # file_size_mb (will update)
            "synthetic_ohlc",                      # data_sources
            "completed",                           # status
            "envelope_top,envelope_bot,pldot,rsi,macd",  # technical_indicators
            "visualization_generator",             # created_by
            "arrayrecord",                         # data_format
            "hourly",                              # time_resolution
            "ohlc_bars",                          # visualization_type
            True,                                  # is_time_series
            len(data)                             # window_size
        )

        print(f"✅ Created dataset record with ID: {dataset_id}")
        return dataset_id, run_id

    finally:
        await conn.close()

def create_arrayrecord_file(symbol: str, data: list, run_id: int) -> str:
    """Create ArrayRecord file with the training data."""

    # Create directory structure
    output_dir = Path(f"/mnt/d/ats-data/training/run_{run_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{symbol}_visualization_ready.arrayrecord"

    # Write data to ArrayRecord file
    with ArrayRecordWriter(str(output_file), "compress") as writer:
        for bar in data:
            # Convert to JSON bytes
            json_data = json.dumps(bar).encode('utf-8')
            writer.write(json_data)

    # Get file size
    file_size_mb = output_file.stat().st_size / (1024 * 1024)

    print(f"✅ Created ArrayRecord file: {output_file}")
    print(f"📊 File size: {file_size_mb:.2f} MB")
    print(f"📈 Records: {len(data)}")

    return str(output_file), file_size_mb

async def update_file_size(dataset_id: int, file_size_mb: float):
    """Update the file size in the database."""
    connection_str = "postgresql://postgres:dev_password@localhost:3432/dev_db"

    conn = await asyncpg.connect(connection_str)
    try:
        await conn.execute(
            "UPDATE dev_training_datasets SET file_size_mb = $1 WHERE id = $2",
            file_size_mb, dataset_id
        )
        print(f"✅ Updated dataset {dataset_id} file size: {file_size_mb:.2f} MB")
    finally:
        await conn.close()

async def main():
    """Generate visualization-ready training data for AAPL and TSLA."""
    print("🚀 Generating Visualization-Ready Training Data")
    print("=" * 60)

    symbols = ["AAPL", "TSLA"]

    for symbol in symbols:
        print(f"\n📊 Generating data for {symbol}...")

        # Create OHLC data
        ohlc_data = create_sample_ohlc_data(symbol, num_bars=21)
        print(f"✅ Generated {len(ohlc_data)} OHLC bars for {symbol}")

        # Create database record
        dataset_id, run_id = await create_training_dataset_record(symbol, ohlc_data)

        # Create ArrayRecord file
        file_path, file_size_mb = create_arrayrecord_file(symbol, ohlc_data, run_id)

        # Update file size in database
        await update_file_size(dataset_id, file_size_mb)

        print(f"🎯 Completed {symbol}: Dataset ID {dataset_id}, Run ID {run_id}")

        # Show sample data
        print(f"📋 Sample {symbol} data:")
        sample_bar = ohlc_data[0]
        print(f"   Open: ${sample_bar['open']}, High: ${sample_bar['high']}, Low: ${sample_bar['low']}, Close: ${sample_bar['close']}")
        print(f"   Volume: {sample_bar['volume']:,}, DateTime: {sample_bar['datetime']}")

    print(f"\n🎉 Training data generation completed!")
    print(f"📁 Files created in: /mnt/d/ats-data/training/run_*")
    print(f"🗄️  Database records created in: dev_training_datasets")

if __name__ == "__main__":
    asyncio.run(main())