#!/usr/bin/env python3
"""
Create working training data that can be visualized.
"""
import os
import json
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import subprocess

def create_sample_ohlc_data(symbol: str, num_bars: int = 21):
    """Create sample OHLC data for visualization."""
    np.random.seed(42 if symbol == 'AAPL' else 123)
    
    base_price = 180.0 if symbol == 'AAPL' else 250.0
    data = []
    current_price = base_price
    
    for i in range(num_bars):
        open_price = current_price + np.random.normal(0, 1.0)
        high_offset = abs(np.random.normal(2.0, 1.0))
        low_offset = -abs(np.random.normal(2.0, 1.0))
        
        high_price = open_price + high_offset
        low_price = open_price + low_offset
        close_price = low_price + (high_price - low_price) * np.random.beta(2, 2)
        current_price = close_price
        
        volume = max(100000, int(np.random.normal(1000000, 200000)))
        envelope_top = high_price * 1.02
        envelope_bot = low_price * 0.98
        pldot = (high_price + low_price) / 2
        
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
            "pldot": round(pldot, 2)
        }
        data.append(bar_data)
    
    return data

def main():
    print("🚀 Creating Working Training Data")
    
    run_id = int(datetime.now().timestamp())
    output_dir = Path(f"/mnt/d/ats-data/training/run_{run_id}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    for symbol in ["AAPL", "TSLA"]:
        print(f"\n📊 Creating {symbol} data...")
        
        # Generate OHLC data
        ohlc_data = create_sample_ohlc_data(symbol)
        
        # Create JSON data file
        json_file = output_dir / f"{symbol}_data.json"
        with open(json_file, 'w') as f:
            json.dump(ohlc_data, f, indent=2)
        
        # Create ArrayRecord conversion script with correct options
        converter_script = f'''
import json
from array_record.python.array_record_module import ArrayRecordWriter

with open('/data/training/run_{run_id}/{symbol}_data.json', 'r') as f:
    data = json.load(f)

output_file = '/data/training/run_{run_id}/{symbol}_visualization.arrayrecord'
writer = ArrayRecordWriter(output_file, "brotli")  # Use brotli compression

for bar in data:
    json_data = json.dumps(bar).encode('utf-8')
    writer.write(json_data)

writer.close()
print(f"✅ Created {{output_file}} with {{len(data)}} records")
'''
        
        script_file = output_dir / f"convert_{symbol}.py"
        with open(script_file, 'w') as f:
            f.write(converter_script)
        
        # Run conversion in container
        result = subprocess.run([
            "docker", "exec", "ats-dev-analytics",
            "python3", f"/data/training/run_{run_id}/convert_{symbol}.py"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ {result.stdout.strip()}")
            
            # Get file size
            arrayrecord_file = output_dir / f"{symbol}_visualization.arrayrecord"
            file_size = 0.001
            if arrayrecord_file.exists():
                file_size = arrayrecord_file.stat().st_size / (1024 * 1024)
            
            # Create database record
            dataset_name = f"Working_{symbol}_Visualization_{datetime.now().strftime('%H%M%S')}"
            
            insert_query = f"""
INSERT INTO dev_training_datasets (
    dataset_name, run_id, total_sequences, sequence_length, feature_count,
    label_count, symbols, date_range_start, date_range_end, data_quality_score,
    feature_completeness, label_completeness, file_size_mb, data_sources,
    status, technical_indicators, created_by, data_format
) VALUES (
    '{dataset_name}', {run_id}, {len(ohlc_data)}, {len(ohlc_data)}, {len(ohlc_data[0].keys())},
    0, ARRAY['{symbol}'], '2025-08-01', '2025-08-21', 1.0, 1.0, 1.0, {file_size},
    'synthetic_ohlc', 'completed', 'envelope_top,envelope_bot,pldot', 
    'working_generator', 'arrayrecord'
);
"""
            
            db_result = subprocess.run([
                "python3", "scripts/run_dev.py", "query", "--query", insert_query
            ], capture_output=True, text=True)
            
            if db_result.returncode == 0:
                print(f"✅ Database record created for {symbol}")
            else:
                print(f"⚠️  Database insert issue: {db_result.stderr}")
            
            # Show sample data
            sample = ohlc_data[0]
            print(f"📋 Sample: Open=${sample['open']}, High=${sample['high']}, Low=${sample['low']}, Close=${sample['close']}")
            
        else:
            print(f"❌ ArrayRecord creation failed: {result.stderr}")
    
    print(f"\n🎯 Completed! Run ID: {run_id}")
    
    # Verify datasets
    check_result = subprocess.run([
        "python3", "scripts/run_dev.py", "query", "--query",
        "SELECT id, dataset_name, symbols, file_size_mb FROM dev_training_datasets ORDER BY id DESC LIMIT 5;"
    ], capture_output=True, text=True)
    
    if check_result.returncode == 0:
        print("📊 Current datasets:")
        print(check_result.stdout)

if __name__ == "__main__":
    main()