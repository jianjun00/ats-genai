#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install asyncpg
        
        echo "🚀 Step 2: Populating instrument data..."
        python /scripts/populate_instrument.py
        
        echo "✅ Instrument population completed!"
    volumeMounts:
    - name: script-volume
      mountPath: /scripts
  volumes:
  - name: script-volume
    configMap:
      name: populate-instrument-script
  restartPolicy: Never
backoffLimit: 3
