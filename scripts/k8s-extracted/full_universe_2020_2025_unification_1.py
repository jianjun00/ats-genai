#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install asyncpg psutil
        
        echo "🔧 Running full universe 2020-2025 price unification..."
        cd /scripts
        python run_full_universe_unification.py
        
        echo "✅ Full universe unification completed!"
    env:
    - name: START_DATE
      value: "2020-01-01"
    - name: END_DATE
      value: "2025-08-19"
    - name: BATCH_SIZE
      value: "25"
    - name: SYMBOL_LIMIT
      value: "500"  # Process 500 symbols
    - name: SKIP_EXISTING
      value: "true"
    - name: FOCUS_RECENT
      value: "true"  # Focus on 2023-2025 for better performance
    resources:
      requests:
        memory: "4Gi"
        cpu: "2000m"
      limits:
        memory: "8Gi"
        cpu: "4000m"
    volumeMounts:
    - name: script-volume
      mountPath: /scripts
  volumes:
  - name: script-volume
    configMap:
      name: full-universe-price-unification-script
  restartPolicy: Never
backoffLimit: 1
