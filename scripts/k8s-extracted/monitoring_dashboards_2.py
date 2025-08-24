#!/usr/bin/env python3

echo "📦 Installing dependencies..."
            pip install asyncpg
            
            echo "📊 Generating daily monitoring report..."
            cd /scripts
            python generate_monitoring_report.py
            
            echo "✅ Daily monitoring report completed!"
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
        volumeMounts:
        - name: script-volume
          mountPath: /scripts
      volumes:
      - name: script-volume
        configMap:
          name: monitoring-dashboards-script
      restartPolicy: OnFailure
successfulJobsHistoryLimit: 3
failedJobsHistoryLimit: 2
