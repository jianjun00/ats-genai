#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install asyncpg psutil
        
        echo "🌍 Running IMMEDIATE FULL UNIVERSE FIX..."
        cd /scripts
        python run_immediate_full_universe.py
        
        echo "✅ Full universe fix completed!"
    env:
    - name: START_DATE
      value: "2024-01-01"  # Focus on recent data first
    - name: END_DATE
      value: "2025-08-19"
    - name: BATCH_SIZE
      value: "100"  # Large batches for efficiency
    - name: SKIP_EXISTING
      value: "true"  # Skip existing for speed
    resources:
      requests:
        memory: "8Gi"
        cpu: "4000m"
      limits:
        memory: "16Gi"
        cpu: "8000m"
    volumeMounts:
    - name: script-volume
      mountPath: /scripts
  volumes:
  - name: script-volume
    configMap:
      name: immediate-full-universe-fix-script
  restartPolicy: Never
backoffLimit: 2
