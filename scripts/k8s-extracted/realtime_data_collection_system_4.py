#!/usr/bin/env python3

echo "🔄 Running weekly comprehensive backfill..."
            cd /backfill
            python comprehensive_backfill.py
            
            echo "✅ Weekly backfill completed!"
        env:
        - name: BACKFILL_DAYS
          value: "7"
        - name: MAX_SYMBOLS_PER_BATCH
          value: "100"
        - name: CHUNK_SIZE_HOURS
          value: "24"
          
        # API credentials
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
            memory: "8Gi"
            cpu: "4000m"
          limits:
            memory: "16Gi"
            cpu: "8000m"
            
      restartPolicy: OnFailure
      
successfulJobsHistoryLimit: 2
failedJobsHistoryLimit: 1

---
