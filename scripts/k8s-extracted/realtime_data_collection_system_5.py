#!/usr/bin/env python3

echo "📊 Starting data quality monitor..."
        cd /monitor
        python quality_monitor.py
    env:
    - name: ALERT_THRESHOLD_ACCURACY
      value: "0.9"
    - name: ALERT_THRESHOLD_LATENCY
      value: "300"  # 5 minutes
    - name: CHECK_INTERVAL_SECONDS
      value: "300"  # 5 minutes
      
    # Database configuration
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
      
    resources:
      requests:
        memory: "256Mi"
        cpu: "100m"
      limits:
        memory: "512Mi"
        cpu: "200m"
        
    # Lightweight health checks
    livenessProbe:
      httpGet:
        path: /health
        port: 8080
      initialDelaySeconds: 30
      periodSeconds: 60
      
---
