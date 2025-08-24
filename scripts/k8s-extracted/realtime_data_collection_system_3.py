#!/usr/bin/env python3

echo "🔍 Running gap detection..."
            cd /gap-detection
            python gap_detector.py
            
            echo "✅ Gap detection completed!"
        env:
        - name: MAX_GAP_MINUTES
          value: "10"
        - name: BACKFILL_PRIORITY_THRESHOLD
          value: "3"
        - name: MAX_CONCURRENT_BACKFILLS
          value: "5"
          
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
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
            
      restartPolicy: OnFailure
      
successfulJobsHistoryLimit: 3
failedJobsHistoryLimit: 2

---
