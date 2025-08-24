#!/usr/bin/env python3

pip install fastapi uvicorn asyncpg pydantic pandas numpy
        cd /app
        python webapp.py
    ports:
    - containerPort: 5000
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
    - name: webapp-config
      mountPath: /app
    resources:
      requests:
        memory: "512Mi"
        cpu: "250m"
      limits:
        memory: "1Gi"
        cpu: "500m"
    readinessProbe:
      httpGet:
        path: /health
        port: 5000
      initialDelaySeconds: 10
      periodSeconds: 10
    livenessProbe:
      httpGet:
        path: /health
        port: 5000
      initialDelaySeconds: 30
      periodSeconds: 30
  volumes:
  - name: webapp-config
    configMap:
      name: enhanced-dataset-visualization-real-data-config
      defaultMode: 0755
