#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install asyncpg
        
        echo "🔧 Setting up monitoring dashboards..."
        cd /scripts
        python setup_monitoring_views.py
        
        echo "📊 Generating initial monitoring report..."
        python generate_monitoring_report.py
        
        echo "✅ Monitoring dashboard setup completed!"
    resources:
      requests:
        memory: "1Gi"
        cpu: "500m"
      limits:
        memory: "2Gi"
        cpu: "1000m"
    volumeMounts:
    - name: script-volume
      mountPath: /scripts
  volumes:
  - name: script-volume
    configMap:
      name: monitoring-dashboards-script
  restartPolicy: Never
backoffLimit: 2
