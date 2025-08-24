#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install asyncpg pandas numpy
        
        echo "🚀 Starting 2022-2025 Comprehensive Backtest (FIXED)..."
        python /scripts/run_backtest.py
        
        echo "✅ Backtest completed!"
    volumeMounts:
    - name: script-volume
      mountPath: /scripts
    resources:
      requests:
        memory: "1Gi"
        cpu: "500m"
      limits:
        memory: "2Gi"
        cpu: "1000m"
  volumes:
  - name: script-volume
    configMap:
      name: backtest-2022-2025-fixed-script
  restartPolicy: Never
backoffLimit: 2
