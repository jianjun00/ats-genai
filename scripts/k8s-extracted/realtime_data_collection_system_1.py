#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install asyncpg aiohttp websockets pytz
        
        echo "🚀 Starting real-time streaming collector..."
        cd /app
        python streaming_collector.py
    env:
    # Market configuration
    - name: UNIVERSE_SIZE
      value: "2000"
    - name: MARKET_HOURS_ONLY
      value: "true"
    - name: ENABLE_PREMARKET
      value: "false"
    - name: ENABLE_AFTERHOURS
      value: "true"
    - name: MAX_LATENCY_SECONDS
      value: "120"
    
    # API Keys (from secrets in production)
    - name: POLYGON_API_KEY
      valueFrom:
        secretKeyRef:
          name: api-credentials
          key: polygon-api-key
    - name: TIINGO_API_KEY
      valueFrom:
        secretKeyRef:
          name: api-credentials
          key: tiingo-api-key
    - name: FMP_API_KEY
      valueFrom:
        secretKeyRef:
          name: api-credentials
          key: fmp-api-key
          
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
        memory: "4Gi"
        cpu: "2000m"
      limits:
        memory: "8Gi"
        cpu: "4000m"
        
    # Health checks
    livenessProbe:
      exec:
        command:
        - python
        - -c
        - "import psutil; exit(0 if any('streaming_collector' in p.name() for p in psutil.process_iter()) else 1)"
      initialDelaySeconds: 30
      periodSeconds: 60
      
    readinessProbe:
      exec:
        command:
        - python
        - -c
        - "import asyncpg, asyncio; loop = asyncio.get_event_loop(); conn = loop.run_until_complete(asyncpg.connect('postgresql://postgres:dev_password@postgres-simple:5432/dev_db')); loop.run_until_complete(conn.close()); exit(0)"
      initialDelaySeconds: 10
      periodSeconds: 30
      
    volumeMounts:
    - name: app-code
      mountPath: /app
      
  volumes:
  - name: app-code
    configMap:
      name: realtime-collector-config
      
  # Ensure database is ready
  initContainers:
  - name: db-check
    image: postgres:15
    command: ['pg_isready', '-h', 'postgres-simple', '-p', '5432', '-U', 'postgres']
    
  restartPolicy: Always
  
---
