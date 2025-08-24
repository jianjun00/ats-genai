#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install fastapi uvicorn asyncpg numpy
        echo "🚀 Starting Enhanced Dataset Detail Webapp..."
        cd /config
        python unified_analytics_fixed.py
    ports:
    - containerPort: 5000
    env:
    - name: PYTHONPATH
      value: "/config"
    - name: ENVIRONMENT
      value: "dev"
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
    - name: config-volume
      mountPath: /config
    - name: training-data
      mountPath: /config/training_data_output
      readOnly: true
    resources:
      requests:
        memory: "256Mi"
        cpu: "250m"
      limits:
        memory: "1Gi"
        cpu: "1000m"
    readinessProbe:
      httpGet:
        path: /health
        port: 5000
      initialDelaySeconds: 10
      periodSeconds: 5
    livenessProbe:
      httpGet:
        path: /health
        port: 5000
      initialDelaySeconds: 30
      periodSeconds: 10
  volumes:
  - name: config-volume
    configMap:
      name: enhanced-dataset-detail-webapp-config
  - name: training-data
    hostPath:
      path: /home/jianjun/ats-genai/training_data_output
      type: Directory
