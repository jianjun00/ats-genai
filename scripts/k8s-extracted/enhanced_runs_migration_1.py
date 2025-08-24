#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install asyncpg
        
        echo "🔧 Running enhanced runs table migration..."
        python /scripts/run_migration.py
        
        echo "✅ Migration job completed!"
    volumeMounts:
    - name: script-volume
      mountPath: /scripts
  volumes:
  - name: script-volume
    configMap:
      name: enhanced-runs-migration-script
  restartPolicy: Never
backoffLimit: 3
