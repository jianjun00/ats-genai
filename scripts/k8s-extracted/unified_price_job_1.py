#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install asyncpg
        
        echo "🔧 Running price unification job..."
        cd /scripts
        python run_price_unification.py
        
        echo "✅ Price unification job completed!"
    env:
    - name: SYMBOLS
      value: "AAPL,MSFT,GOOGL"
    - name: TARGET_DATE
      value: "2025-08-15"
    - name: LIMIT
      value: "3"
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
      name: unified-price-job-script
  restartPolicy: Never
backoffLimit: 1
