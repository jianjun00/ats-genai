#!/usr/bin/env python3

echo "Installing dependencies..."
      pip install --no-cache-dir asyncpg
      
      echo "Running dynamic universe update..."
      cd /app
      python src/universe/dynamic_modeling_universe.py --update-daily --debug
      
      echo "Generating universe report..."
      python src/universe/dynamic_modeling_universe.py --report
      
      echo "Universe update completed successfully"
    
    env:
    - name: ENVIRONMENT
      value: "dev"
    - name: DB_HOST
      value: "postgres"
    - name: DB_PORT
      value: "5432"
    - name: DB_USER
      valueFrom:
        secretKeyRef:
          name: postgres-secret
          key: username
    - name: DB_PASSWORD
      valueFrom:
        secretKeyRef:
          name: postgres-secret
          key: password
    - name: DB_NAME
      value: "dev_db"
    - name: PYTHONPATH
      value: "src"
    
    volumeMounts:
    - name: app-code
      mountPath: /app
    - name: config
      mountPath: /app/config
    
    resources:
      requests:
        memory: "256Mi"
        cpu: "250m"
      limits:
        memory: "512Mi"
        cpu: "500m"
  
  volumes:
  - name: app-code
    configMap:
      name: dynamic-universe-code
  - name: config
    configMap:
      name: app-config
