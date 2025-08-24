#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install asyncpg psutil
        
        echo "🔧 Running comprehensive 2020-2025 price unification..."
        cd /scripts
        python run_comprehensive_unification.py
        
        echo "✅ Comprehensive unification completed!"
    env:
    - name: START_DATE
      value: "2020-01-01"
    - name: END_DATE
      value: "2025-08-19"
    - name: BATCH_SIZE
      value: "20"
    - name: SYMBOL_LIMIT
      value: "10"  # Start with 10 symbols for testing
    - name: SKIP_EXISTING
      value: "true"
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
      name: comprehensive-price-unification-script
  restartPolicy: Never
backoffLimit: 1
