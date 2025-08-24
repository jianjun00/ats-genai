#!/usr/bin/env python3

echo "🚀 Starting Enhanced Analytics Webapp..."
      pip install --no-cache-dir fastapi uvicorn asyncpg pydantic
      
      cat > /app/webapp.py << 'EOF'
      from fastapi import FastAPI
      import uvicorn
      
      app = FastAPI(title="Enhanced Analytics Webapp", version="1.0")
      
      @app.get("/")
      async def root():
          return {"message": "Enhanced Analytics Webapp", "status": "running"}
          
      @app.get("/health")
      async def health():
          return {"status": "healthy"}
      
      if __name__ == "__main__":
          uvicorn.run(app, host="0.0.0.0", port=3000)
      EOF
      
      cd /app && python webapp.py
    env:
    - name: ENVIRONMENT
      value: "dev"
    - name: DB_HOST
      value: "postgres-simple"
    - name: DB_PORT
      value: "5432"
    - name: DB_USER
      valueFrom:
        secretKeyRef:
          name: db-credentials-dev
          key: DB_USER
          optional: true
    - name: DB_PASSWORD
      valueFrom:
        secretKeyRef:
          name: db-credentials-dev
          key: DB_PASSWORD
          optional: true
    - name: DB_NAME
      valueFrom:
        secretKeyRef:
          name: db-credentials-dev
          key: DB_NAME
          optional: true
    resources:
      requests:
        memory: "512Mi"
        cpu: "250m"
      limits:
        memory: "1Gi"
        cpu: "500m"
