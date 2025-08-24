#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install asyncpg
        
        echo "🔧 Running database migrations..."
        python /scripts/run_migrations.py
        
        echo "✅ Migration job completed!"
    volumeMounts:
    - name: script-volume
      mountPath: /scripts
  volumes:
  - name: script-volume
    configMap:
      name: database-migration-script
  restartPolicy: Never
backoffLimit: 3
