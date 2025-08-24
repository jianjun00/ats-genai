#!/usr/bin/env python3

echo "📦 Installing dependencies..."
            pip install asyncpg aiohttp statistics
            
            echo "🔍 Running daily real-time validation..."
            cd /validation
            python realtime_batch_validator.py
            
            echo "✅ Daily validation completed!"
        env:
        # Validation configuration
        - name: VALIDATION_BATCH_SIZE
          value: "50"
        - name: MAX_LATENCY_MINUTES
          value: "5.0"
        - name: MAX_PRICE_DIFF_PCT
          value: "0.5"
        - name: MIN_ACCURACY_SCORE
          value: "0.95"
        - name: MIN_COMPLETENESS
          value: "0.90"
        
        # Override validation date for testing
        # - name: VALIDATION_DATE
        #   value: "2025-08-22"
        
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
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
            
        volumeMounts:
        - name: validation-code
          mountPath: /validation
          
      volumes:
      - name: validation-code
        configMap:
          name: validation-scripts
          
      restartPolicy: OnFailure
      
successfulJobsHistoryLimit: 5
failedJobsHistoryLimit: 3

---
