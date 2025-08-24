#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install asyncpg
        
        echo "🎯 Running final coverage catalog test..."
        cd /scripts
        python final_coverage_test.py
        
        echo "✅ Final coverage catalog test completed!"
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
      name: final-coverage-test-script
  restartPolicy: Never
backoffLimit: 1
