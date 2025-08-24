#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install asyncpg
        
        echo "🔧 Creating job_runs table..."
        python /scripts/create_job_runs_table.py
        
        echo "✅ Job runs table creation completed!"
    volumeMounts:
    - name: script-volume
      mountPath: /scripts
  volumes:
  - name: script-volume
    configMap:
      name: job-runs-migration-script
  restartPolicy: Never
backoffLimit: 3
