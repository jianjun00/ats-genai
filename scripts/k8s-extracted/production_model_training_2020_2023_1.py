#!/usr/bin/env python3

set -e
      echo "🚀 Starting Production Model Training Job"
      echo "=========================================="
      echo "Environment: $ENVIRONMENT"
      echo "Database: $DB_HOST:$DB_PORT/$DB_NAME"
      echo "Memory Limit: 16Gi"
      echo "CPU Limit: 8 cores"
      echo "Training Period: 2020-2023"
      echo "Target: All available instruments"
      echo ""
      
      echo "📦 Installing dependencies..."
      pip install --no-cache-dir \
        asyncpg \
        pandas \
        numpy \
        scikit-learn \
        torch \
        xgboost \
        gin-config \
        yfinance \
        ray[default] \
        psutil \
        matplotlib \
        seaborn
      
      echo "🔧 Setting up environment..."
      echo "Python path: $PYTHONPATH"
      echo "Working directory: $(pwd)"
      echo "Available memory: $(free -h)"
      echo "Available CPU: $(nproc)"
      echo ""
      
      echo "🧠 Starting model training..."
      python train_production_model_2020_2023.py
      
      echo "✅ Training job completed successfully!"
    volumeMounts:
    - name: app-code
      mountPath: /app
    - name: model-storage
      mountPath: /app/models
  volumes:
  - name: app-code
    configMap:
      name: ats-training-code
  - name: model-storage
    persistentVolumeClaim:
      claimName: model-storage-pvc
