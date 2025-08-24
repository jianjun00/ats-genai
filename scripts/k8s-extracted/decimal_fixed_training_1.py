#!/usr/bin/env python3
import asyncio

set -e
      echo "🚀 Starting REAL Model Training with Decimal Fix"
      echo "================================================="
      echo "Database: $DB_HOST:$DB_PORT/$DB_NAME"
      echo "Environment: $ENVIRONMENT"
      echo ""
      
      echo "📦 Installing dependencies..."
      apt-get update && apt-get install -y gcc
      pip install --no-cache-dir \
        asyncpg \
        pandas \
        numpy \
        scikit-learn \
        psutil \
        tqdm
      
      echo "🔧 Testing database connectivity..."
      python -c "
      import asyncio
      import asyncpg
      import os
      
      async def test_connection():
          db_url = f'postgresql://{os.environ[\"DB_USER\"]}:{os.environ[\"DB_PASSWORD\"]}@{os.environ[\"DB_HOST\"]}:{os.environ[\"DB_PORT\"]}/{os.environ[\"DB_NAME\"]}'
          print(f'Connecting to: {db_url}')
          
          conn = await asyncpg.connect(db_url)
          
          # Test schema access
          instruments = await conn.fetchval('SELECT COUNT(*) FROM dev_instruments')
          prices = await conn.fetchval('SELECT COUNT(*) FROM dev_daily_prices')
          
          print(f'✅ Connected! Instruments: {instruments:,}, Prices: {prices:,}')
          
          # Test join query with proper schema
          test_join = await conn.fetchval('''
              SELECT COUNT(*) FROM dev_instruments i 
              JOIN dev_daily_prices dp ON i.id = dp.instrument_id 
              WHERE dp.date >= '2020-01-01'
          ''')
          print(f'✅ Join test: {test_join:,} price records with instruments')
          
          # Test decimal handling
          test_decimal = await conn.fetchrow('''
              SELECT open_price, high_price, low_price, close, volume 
              FROM dev_daily_prices 
              LIMIT 1
          ''')
          print(f'✅ Sample price data types: {type(test_decimal[\"open_price\"])}, {type(test_decimal[\"close\"])}')
          
          await conn.close()
          return True
      
      if not asyncio.run(test_connection()):
          exit(1)
      "
      
      echo "🧠 Starting REAL model training with decimal fix..."
      python /app/train_decimal_fixed.py
      
      echo "✅ Real training completed!"
    volumeMounts:
    - name: training-script
      mountPath: /app
    - name: model-storage
      mountPath: /app/models
  volumes:
  - name: training-script
    configMap:
      name: decimal-fixed-training-script
  - name: model-storage
    persistentVolumeClaim:
      claimName: model-storage-pvc
