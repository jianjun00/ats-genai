#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install asyncpg
        
        echo "📊 Running coverage data population..."
        cd /scripts
        python populate_coverage_data.py
        
        echo "✅ Coverage data population completed!"
    env:
    - name: DB_HOST
      value: "postgres-simple"
    - name: DB_PORT
      value: "5432"
    - name: DB_USER
      value: "postgres"
    - name: DB_PASSWORD
      value: "dev_password"
    - name: DB_NAME
      value: "dev_db"
    volumeMounts:
    - name: script-volume
      mountPath: /scripts
  volumes:
  - name: script-volume
    configMap:
      name: populate-coverage-data-script
  restartPolicy: Never
backoffLimit: 1
