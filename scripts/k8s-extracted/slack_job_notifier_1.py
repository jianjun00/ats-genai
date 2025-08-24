#!/usr/bin/env python3

echo "📦 Installing dependencies..."
        pip install aiohttp asyncio-subprocess
        
        echo "🔧 Installing kubectl..."
        curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
        chmod +x kubectl
        mv kubectl /usr/local/bin/
        
        echo "🚀 Starting Slack job notifier..."
        python /scripts/slack_notifier.py
    env:
    - name: SLACK_WEBHOOK_URL
      valueFrom:
        secretKeyRef:
          name: slack-credentials
          key: webhook_url
          optional: false
    - name: SLACK_CHANNEL
      value: "#ats-dev-alerts"
    - name: NOTIFICATION_INTERVAL_HOURS
      value: "4"
    volumeMounts:
    - name: script-volume
      mountPath: /scripts
    resources:
      requests:
        memory: "256Mi"
        cpu: "100m"
      limits:
        memory: "512Mi"
        cpu: "200m"
  volumes:
  - name: script-volume
    configMap:
      name: slack-job-notifier-script
  restartPolicy: Always
