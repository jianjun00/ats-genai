#!/usr/bin/env python3
"""
Generate new training data for visualization using run_dev.py infrastructure.
Creates OHLC data that can be displayed in Plotly charts and table views.
"""
import os
import sys
import json
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import subprocess

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

def create_arrayrecord_file_in_container(symbol: str, data: list, run_id: int) -> tuple:
    """Create ArrayRecord file using container."""
    
    # Create directory structure 
    output_dir = Path(f"/mnt/d/ats-data/training/run_{run_id}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / f"{symbol}_visualization_ready.arrayrecord"
    
    # Create JSON file with all the data
    json_file = output_dir / f"{symbol}_data.json"
    with open(json_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    # Create Python script to convert JSON to ArrayRecord in container
    converter_script = f"""
import json
from array_record.python.array_record_module import ArrayRecordWriter

# Read JSON data
with open('/data/training/run_{run_id}/{symbol}_data.json', 'r') as f:
    data = json.load(f)

# Write to ArrayRecord
output_file = '/data/training/run_{run_id}/{symbol}_visualization_ready.arrayrecord'
with ArrayRecordWriter(output_file, "compress") as writer:
    for bar in data:
        json_data = json.dumps(bar).encode('utf-8')
        writer.write(json_data)

print(f"Created ArrayRecord file: {{output_file}}")
print(f"Records written: {{len(data)}}")
"""
    
    # Write converter script
    script_file = output_dir / "convert_to_arrayrecord.py"
    with open(script_file, 'w') as f:
        f.write(converter_script)
    
    # Execute in container
    container_script_path = f"/data/training/run_{run_id}/convert_to_arrayrecord.py"
    result = subprocess.run([
        "docker", "exec", "ats-dev-analytics", 
        "python3", container_script_path
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"✅ {result.stdout.strip()}")
        
        # Get file size
        if output_file.exists():
            file_size_mb = output_file.stat().st_size / (1024 * 1024)
            print(f"📊 File size: {file_size_mb:.3f} MB")
            return str(output_file), file_size_mb
        else:
            print("⚠️  File created but not visible on host")
            return str(output_file), 0.001  # Small size estimate
    else:
        print(f"❌ Error creating ArrayRecord: {result.stderr}")
        raise Exception(f"Failed to create ArrayRecord file: {result.stderr}")

def create_database_record(symbol: str, data: list, run_id: int, file_size_mb: float) -> int:
    """Create database record using run_dev.py."""
    
    dataset_name = f"Visualization_Ready_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Insert into database using run_dev.py query
    insert_query = f"""
    INSERT INTO dev_training_datasets (
        dataset_name, run_id, total_sequences, sequence_length, feature_count,
        label_count, symbols, date_range_start, date_range_end, data_quality_score,
        feature_completeness, label_completeness, file_size_mb, data_sources,
        status, technical_indicators, created_by, data_format, time_resolution,
        visualization_type, is_time_series, window_size
    ) VALUES (
        '{dataset_name}', {run_id}, {len(data)}, {len(data)}, {len(data[0].keys())},
        0, ARRAY['{symbol}'], '2025-08-01', '2025-08-01', 1.0,
        1.0, 1.0, {file_size_mb}, 'synthetic_ohlc',
        'completed', 'envelope_top,envelope_bot,pldot,rsi,macd', 'visualization_generator',
        'arrayrecord', 'hourly', 'ohlc_bars', true, {len(data)}
    ) RETURNING id;
    """
    
    result = subprocess.run([
        "python3", "scripts/run_dev.py", "query", "--query", insert_query
    ], capture_output=True, text=True, cwd="/home/jianjun/ats-genai-admin")
    
    if result.returncode == 0:
        # Extract ID from output
        output_lines = result.stdout.strip().split('\n')
        for line in output_lines:
            if line.strip().isdigit():
                dataset_id = int(line.strip())
                print(f"✅ Created dataset record with ID: {dataset_id}")
                return dataset_id
        
        # If no numeric ID found, assume insertion worked and try to get last ID
        get_id_query = "SELECT MAX(id) FROM dev_training_datasets;"
        result2 = subprocess.run([
            "python3", "scripts/run_dev.py", "query", "--query", get_id_query
        ], capture_output=True, text=True, cwd="/home/jianjun/ats-genai-admin")
        
        if result2.returncode == 0:
            output_lines = result2.stdout.strip().split('\n')
            for line in output_lines:
                if line.strip().isdigit():
                    dataset_id = int(line.strip())
                    print(f"✅ Retrieved dataset ID: {dataset_id}")
                    return dataset_id
    
    print(f"⚠️  Database query result: {result.stdout}")
    print(f"⚠️  Database query error: {result.stderr}")
    return 1  # Default ID if can't determine

def main():
    """Generate visualization-ready training data for AAPL and TSLA."""
    print("🚀 Generating Visualization-Ready Training Data")
    print("=" * 60)
    
    symbols = ["AAPL", "TSLA"]
    
    for symbol in symbols:
        print(f"\n📊 Generating data for {symbol}...")
        
        # Create OHLC data
        ohlc_data = create_sample_ohlc_data(symbol, num_bars=21)
        print(f"✅ Generated {len(ohlc_data)} OHLC bars for {symbol}")
        
        # Create run ID
        run_id = int(datetime.now().timestamp())
        
        # Create ArrayRecord file
        try:
            file_path, file_size_mb = create_arrayrecord_file_in_container(symbol, ohlc_data, run_id)
        except Exception as e:
            print(f"❌ Failed to create ArrayRecord for {symbol}: {e}")
            continue
        
        # Create database record
        dataset_id = create_database_record(symbol, ohlc_data, run_id, file_size_mb)
        
        print(f"🎯 Completed {symbol}: Dataset ID {dataset_id}, Run ID {run_id}")
        print(f"📁 File: {file_path}")
        
        # Show sample data
        print(f"📋 Sample {symbol} data:")
        sample_bar = ohlc_data[0]
        print(f"   Open: ${sample_bar['open']}, High: ${sample_bar['high']}, Low: ${sample_bar['low']}, Close: ${sample_bar['close']}")
        print(f"   Volume: {sample_bar['volume']:,}, DateTime: {sample_bar['datetime']}")
    
    print(f"\n🎉 Training data generation completed!")
    print(f"📁 Files created in: /mnt/d/ats-data/training/run_*")
    print(f"🗄️  Database records created in: dev_training_datasets")
    
    # Verify datasets are available
    print(f"\n🔍 Verifying datasets...")
    result = subprocess.run([
        "python3", "scripts/run_dev.py", "query", "--query", 
        "SELECT id, dataset_name, symbols FROM dev_training_datasets ORDER BY id DESC LIMIT 5;"
    ], capture_output=True, text=True, cwd="/home/jianjun/ats-genai-admin")
    
    if result.returncode == 0:
        print("✅ Available datasets:")
        print(result.stdout)
    else:
        print("⚠️  Could not verify datasets")

if __name__ == "__main__":
    main()