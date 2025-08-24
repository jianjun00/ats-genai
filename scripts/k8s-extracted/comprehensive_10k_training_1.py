#!/usr/bin/env python3
import asyncio

set -e
      echo "🚀 Starting COMPREHENSIVE Training Data Generation for ALL 10K Instruments"
      echo "=========================================================================="
      echo "Database: $DB_HOST:$DB_PORT/$DB_NAME"
      echo "Target: ALL available instruments (up to 10,000)"
      echo "Memory: 24Gi, CPU: 12 cores"
      echo ""
      
      echo "📦 Installing dependencies..."
      apt-get update && apt-get install -y gcc g++
      pip install --no-cache-dir \
        asyncpg \
        pandas \
        numpy \
        scikit-learn \
        xgboost \
        psutil \
        tqdm \
        joblib
      
      echo "🔧 Testing comprehensive database connectivity..."
      python -c "
      import asyncio
      import asyncpg
      import os
      
      async def comprehensive_db_test():
          db_url = f'postgresql://{os.environ[\"DB_USER\"]}:{os.environ[\"DB_PASSWORD\"]}@{os.environ[\"DB_HOST\"]}:{os.environ[\"DB_PORT\"]}/{os.environ[\"DB_NAME\"]}'
          print(f'Connecting to: {db_url}')
          
          conn = await asyncpg.connect(db_url)
          
          # Get comprehensive statistics
          total_instruments = await conn.fetchval('SELECT COUNT(*) FROM dev_instruments')
          total_prices = await conn.fetchval('SELECT COUNT(*) FROM dev_daily_prices')
          
          # Get coverage by year
          coverage_2020_2023 = await conn.fetchval('''
              SELECT COUNT(DISTINCT dp.instrument_id) 
              FROM dev_daily_prices dp 
              WHERE dp.date >= '2020-01-01' AND dp.date <= '2023-12-31'
          ''')
          
          # Get broader coverage (all available data)
          all_time_coverage = await conn.fetchval('''
              SELECT COUNT(DISTINCT dp.instrument_id) 
              FROM dev_daily_prices dp
          ''')
          
          # Get date range of available data
          date_range = await conn.fetchrow('''
              SELECT MIN(date) as earliest, MAX(date) as latest, COUNT(*) as total_records
              FROM dev_daily_prices
          ''')
          
          print(f'✅ Connected! Total Instruments: {total_instruments:,}')
          print(f'📊 Total Price Records: {total_prices:,}')
          print(f'🎯 2020-2023 Coverage: {coverage_2020_2023} instruments')
          print(f'📈 All-Time Coverage: {all_time_coverage} instruments')
          print(f'📅 Data Range: {date_range[\"earliest\"]} to {date_range[\"latest\"]} ({date_range[\"total_records\"]:,} records)')
          
          # Test comprehensive query
          comprehensive_test = await conn.fetch('''
              SELECT 
                  i.id as instrument_id,
                  i.symbol,
                  COUNT(dp.date) as price_count,
                  MIN(dp.date) as first_date,
                  MAX(dp.date) as last_date
              FROM dev_instruments i
              JOIN dev_daily_prices dp ON i.id = dp.instrument_id
              GROUP BY i.id, i.symbol
              HAVING COUNT(dp.date) >= 50  -- At least 50 trading days
              ORDER BY COUNT(dp.date) DESC
              LIMIT 10
          ''')
          
          print(f'🔍 Top instruments by data availability:')
          for row in comprehensive_test:
              print(f'   {row[\"symbol\"]}: {row[\"price_count\"]} days ({row[\"first_date\"]} to {row[\"last_date\"]})')
          
          await conn.close()
          return True
      
      if not asyncio.run(comprehensive_db_test()):
          exit(1)
      "
      
      echo "🧠 Starting COMPREHENSIVE model training for ALL available instruments..."
      python /app/train_comprehensive_10k.py
      
      echo "✅ Comprehensive training completed!"
    volumeMounts:
    - name: training-script
      mountPath: /app
    - name: model-storage
      mountPath: /app/models
  volumes:
  - name: training-script
    configMap:
      name: comprehensive-10k-training-script
  - name: model-storage
    persistentVolumeClaim:
      claimName: model-storage-pvc
