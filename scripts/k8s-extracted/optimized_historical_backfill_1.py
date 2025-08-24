#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install asyncpg psutil
        
        echo "🔧 Running optimized historical 2020-2022 backfill..."
        cd /scripts
        python run_optimized_historical_backfill.py
        
        echo "✅ Optimized historical backfill completed!"
    env:
    - name: START_DATE
      value: "2020-01-01"
    - name: END_DATE
      value: "2022-12-31"
    - name: BATCH_SIZE
      value: "10"  # Smaller batches for faster processing
    - name: SYMBOL_LIMIT
      value: "50"  # Start with 50 major symbols
    - name: SKIP_EXISTING
      value: "false"  # Don't skip for comprehensive backfill
    resources:
      requests:
        memory: "2Gi"
        cpu: "1000m"
      limits:
        memory: "4Gi"
        cpu: "2000m"
    volumeMounts:
    - name: script-volume
      mountPath: /scripts
  volumes:
  - name: script-volume
    configMap:
      name: optimized-historical-backfill-script
  restartPolicy: Never
backoffLimit: 2
