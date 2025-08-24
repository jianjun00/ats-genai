#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install --no-cache-dir asyncpg aiohttp
        
        echo "🔧 Running Enhanced FMP Backfill with Exponential Backoff..."
        cd /app
        python enhanced_fmp_backfill_with_retry.py
        
        echo "✅ Enhanced FMP Backfill completed!"
    resources:
      requests:
        memory: "512Mi"
        cpu: "500m"
      limits:
        memory: "2Gi"
        cpu: "1500m"
    volumeMounts:
    - name: script-volume
      mountPath: /app
  volumes:
  - name: script-volume
    configMap:
      name: enhanced-fmp-backfill-script
      defaultMode: 0755
