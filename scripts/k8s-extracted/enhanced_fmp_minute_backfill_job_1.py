#!/usr/bin/env python3

echo "📦 Installing dependencies for minute data volume..."
        pip install --no-cache-dir asyncpg aiohttp
        
        echo "🚀 Starting Enhanced FMP Minute Backfill"
        echo "📊 Expected volume: 100 symbols × 98,280 records/year × 20 years = ~196M records"
        echo "⚡ This is a TEST RUN with 100 symbols (vs full 10k = 19.6B records)"
        
        cd /app
        python enhanced_fmp_minute_backfill.py
        
        echo "✅ FMP Minute Backfill completed!"
    resources:
      requests:
        memory: "1Gi"
        cpu: "1000m"
      limits:
        memory: "4Gi"      # Higher memory for minute data volume
        cpu: "2000m"       # Higher CPU for processing
    volumeMounts:
    - name: script-volume
      mountPath: /app
  volumes:
  - name: script-volume
    configMap:
      name: enhanced-fmp-minute-backfill-script
      defaultMode: 0755
