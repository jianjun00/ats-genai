#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install asyncpg psutil
        
        echo "🔧 Running manual daily price unification..."
        cd /scripts
        python run_daily_unification.py
        
        echo "✅ Manual daily unification completed!"
    env:
    - name: BATCH_SIZE
      value: "30"
    - name: SYMBOL_LIMIT
      value: "500"  # Smaller for manual testing
    - name: LOOKBACK_DAYS
      value: "1"  # Just today
    - name: SKIP_EXISTING
      value: "false"  # Don't skip for manual runs
    - name: MIN_VENDORS
      value: "1"
    # Override target date for testing
    - name: FORCE_DATE
      value: "2025-08-19"
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
      name: automated-daily-price-script
  restartPolicy: Never
backoffLimit: 1
