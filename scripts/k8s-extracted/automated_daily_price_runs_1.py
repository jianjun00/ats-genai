#!/usr/bin/env python3

echo "📦 Installing dependencies..."
            pip install asyncpg psutil
            
            echo "🔧 Running automated daily price unification..."
            cd /scripts
            python run_daily_unification.py
            
            echo "✅ Automated daily unification completed!"
        env:
        - name: BATCH_SIZE
          value: "30"
        - name: SYMBOL_LIMIT
          value: "2000"  # Large universe for production
        - name: LOOKBACK_DAYS
          value: "3"  # Process last 3 days
        - name: SKIP_EXISTING
          value: "true"  # Skip existing records for efficiency
        - name: MIN_VENDORS
          value: "1"  # Accept single vendor for daily runs
        # Optional: Override target date (leave empty for auto-detection)
        # - name: FORCE_DATE
        #   value: "2025-08-19"
        resources:
          requests:
            memory: "3Gi"
            cpu: "2000m"
          limits:
            memory: "6Gi"
            cpu: "4000m"
        volumeMounts:
        - name: script-volume
          mountPath: /scripts
      volumes:
      - name: script-volume
        configMap:
          name: automated-daily-price-script
      restartPolicy: OnFailure
successfulJobsHistoryLimit: 5
failedJobsHistoryLimit: 3
