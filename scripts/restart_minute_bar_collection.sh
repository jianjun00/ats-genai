#!/bin/bash
# Restart Minute Bar Collection System
# Processes FirstRate daily downloads into organized parquet files

set -e

echo "🚀 Restarting ATS Minute Bar Collection System..."

# Check for today's FirstRate download
TODAY=$(date +%Y%m%d)
STOCK_ZIP="/mnt/d/ats-data/firstrate-data/daily/stock/stock_${TODAY}_1min_adj_split.zip"
ETF_ZIP="/mnt/d/ats-data/firstrate-data/daily/etf/etf_${TODAY}_1min_adj_split.zip"

if [[ -f "$STOCK_ZIP" ]]; then
    echo "✅ Found stock data: $STOCK_ZIP"
    
    # Process key symbols from stock data
    echo "📊 Processing key stock symbols..."
    cd /home/jianjun/ats-genai-model
    
    PYTHONPATH=src uv run python -c "
import zipfile
import pandas as pd
import os
from pathlib import Path
from datetime import datetime

# Process today's FirstRate stock data
zip_path = '$STOCK_ZIP'
key_symbols = ['AAPL', 'TSLA', 'SPY', 'QQQ', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META']

if os.path.exists(zip_path):
    with zipfile.ZipFile(zip_path) as zf:
        files = zf.namelist()
        processed = 0
        
        for filename in files:
            symbol = filename.split('_')[0].upper() if '_' in filename else filename.split('.')[0].upper()
            
            if symbol in key_symbols:
                # Create organized output directory
                output_dir = Path('/mnt/d/ats-data/firstrate-data/daily/$(date +%Y/%m/%d)') / symbol[0]
                output_dir.mkdir(parents=True, exist_ok=True)
                
                try:
                    with zf.open(filename) as f:
                        df = pd.read_csv(f)
                        if len(df) > 0:
                            output_file = output_dir / f'{symbol}_${TODAY}.parquet'
                            df.to_parquet(output_file)
                            print(f'✅ {symbol}: {len(df)} records -> {output_file}')
                            processed += 1
                except Exception as e:
                    print(f'❌ Error processing {symbol}: {e}')
        
        print(f'📊 Processed {processed} symbols from stock data')
"
else
    echo "❌ No stock data found: $STOCK_ZIP"
fi

if [[ -f "$ETF_ZIP" ]]; then
    echo "✅ Found ETF data: $ETF_ZIP"
    echo "📊 Processing ETF data... (implement if needed)"
else
    echo "⚠️ No ETF data found: $ETF_ZIP"
fi

# Update logs
echo "$(date): Minute bar collection restarted" >> /mnt/d/ats-logs/minute-bar-restart.log

echo "✅ Minute bar collection restart completed"
echo "📋 Check processed files: find /mnt/d/ats-data/firstrate-data/daily/$(date +%Y/%m/%d)/ -name '*.parquet'"