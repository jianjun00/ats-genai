#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install asyncpg psutil
        
        echo "🔧 Running historical 2020-2022 backfill..."
        cd /scripts
        python run_historical_backfill.py
        
        echo "✅ Historical backfill completed!"
    env:
    - name: START_DATE
      value: "2020-01-01"
    - name: END_DATE
      value: "2022-12-31"  # Complete 2020-2022 coverage
    - name: BATCH_SIZE
      value: "15"  # Smaller batches for historical processing
    - name: SYMBOL_LIMIT
      value: "1000"  # Large universe for comprehensive backfill
    - name: SKIP_EXISTING
      value: "false"  # Don't skip existing for comprehensive backfill
    - name: MIN_VENDORS
      value: "2"  # Require at least 2 vendors for quality
    resources:
      requests:
        memory: "6Gi"
        cpu: "3000m"
      limits:
        memory: "12Gi"
        cpu: "6000m"
    volumeMounts:
    - name: script-volume
      mountPath: /scripts
  volumes:
  - name: script-volume
    configMap:
      name: historical-2020-2022-backfill-script
  restartPolicy: Never
backoffLimit: 2
