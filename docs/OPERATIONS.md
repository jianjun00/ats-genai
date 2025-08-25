# ⚙️ Operations Guide

**DevOps, Monitoring, Infrastructure Management, and Troubleshooting**

Complete operations guide consolidating all DevOps procedures, monitoring setup, infrastructure management, and troubleshooting resources.

---

## 🎯 Operations Overview

This guide covers all operational aspects of the ATS platform including Kubernetes cluster management, monitoring and alerting, backup and recovery, security operations, and comprehensive troubleshooting procedures.

### **Core Responsibilities**
- **Infrastructure Management** - Kubernetes clusters, networking, storage
- **Monitoring & Alerting** - Prometheus, Grafana, real-time dashboards
- **Security Operations** - Access control, secrets management, compliance
- **Backup & Recovery** - Data protection, disaster recovery planning
- **Performance Optimization** - Resource tuning, capacity planning
- **Incident Response** - Issue triage, root cause analysis, resolution

---

## 🚀 Quick Operations Commands

### **Cluster Status & Health**
```bash
# Overall cluster health
kubectl cluster-info
kubectl get nodes -o wide
kubectl top nodes

# Service status across environments
kubectl get all -n ats-dev
kubectl get all -n ats-intg  
kubectl get all -n ats-prod

# Resource utilization
kubectl top pods -n ats-dev --sort-by=memory
kubectl top pods -n ats-dev --sort-by=cpu
```

### **Database Operations**
```bash
# Database connectivity test
python scripts/run_dev.py query --query "SELECT version()"

# Database performance check
python scripts/run_dev.py query --query "
SELECT 
  schemaname,
  tablename, 
  n_live_tup as row_count,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_stat_user_tables 
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC 
LIMIT 10
"

# Backup database
kubectl exec -n ats-dev deployment/postgres -- pg_dump -U postgres dev_db > backup_$(date +%Y%m%d).sql
```

### **Service Management**
```bash
# Restart services
kubectl rollout restart deployment/analytics-api -n ats-dev
kubectl rollout restart deployment/ml-inference-service -n ats-dev

# View service logs
kubectl logs -f deployment/analytics-api -n ats-dev --tail=100
kubectl logs -f deployment/postgres -n ats-dev --tail=50

# Scale services
kubectl scale deployment analytics-api --replicas=5 -n ats-dev
kubectl scale deployment ml-inference-service --replicas=3 -n ats-dev
```

---

## 📊 Monitoring & Alerting

### **Prometheus Configuration**
```yaml
# monitoring/prometheus-config.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
  namespace: monitoring
data:
  prometheus.yml: |
    global:
      scrape_interval: 15s
      evaluation_interval: 15s
    
    rule_files:
      - "alert_rules.yml"
    
    alerting:
      alertmanagers:
        - static_configs:
            - targets:
              - alertmanager:9093
    
    scrape_configs:
      - job_name: 'kubernetes-pods'
        kubernetes_sd_configs:
          - role: pod
            namespaces:
              names: ['ats-dev', 'ats-intg', 'ats-prod']
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
            action: keep
            regex: true
      
      - job_name: 'postgres-exporter'
        static_configs:
          - targets: ['postgres-exporter:9187']
      
      - job_name: 'redis-exporter' 
        static_configs:
          - targets: ['redis-exporter:9121']
```

### **Critical Alert Rules**
```yaml
# monitoring/alert-rules.yaml
groups:
- name: ats-platform-alerts
  rules:
  - alert: ServiceDown
    expr: up == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "Service {{ $labels.job }} is down"
      description: "Service {{ $labels.job }} has been down for more than 1 minute"

  - alert: HighLatency
    expr: histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m])) > 0.5
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High latency on {{ $labels.job }}"
      description: "95th percentile latency is {{ $value }}s"

  - alert: DatabaseConnectionFailure
    expr: postgres_up == 0
    for: 30s
    labels:
      severity: critical
    annotations:
      summary: "Database connection lost"
      description: "PostgreSQL database is unreachable"

  - alert: HighMemoryUsage
    expr: (container_memory_usage_bytes / container_spec_memory_limit_bytes) > 0.8
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "High memory usage on {{ $labels.pod }}"
      description: "Memory usage is {{ $value | humanizePercentage }}"
```

### **Grafana Dashboards**
```python
# monitoring/dashboards/ats-platform-dashboard.json
{
  "dashboard": {
    "title": "ATS Platform Overview",
    "panels": [
      {
        "title": "API Request Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(http_requests_total[5m])",
            "legendFormat": "{{ method }} {{ handler }}"
          }
        ]
      },
      {
        "title": "Database Performance", 
        "type": "graph",
        "targets": [
          {
            "expr": "postgres_stat_database_tup_inserted",
            "legendFormat": "Inserts"
          },
          {
            "expr": "postgres_stat_database_tup_fetched",
            "legendFormat": "Selects"
          }
        ]
      },
      {
        "title": "ML Model Inference Latency",
        "type": "graph", 
        "targets": [
          {
            "expr": "histogram_quantile(0.95, rate(ml_inference_duration_seconds_bucket[5m]))",
            "legendFormat": "95th percentile"
          }
        ]
      }
    ]
  }
}
```

---

## 🔐 Security Operations

### **Secrets Management**
```bash
# Create database secrets
kubectl create secret generic db-credentials \
  --from-literal=username=postgres \
  --from-literal=password=secure_password \
  --from-literal=url="postgresql://postgres:secure_password@postgres:5432/dev_db" \
  -n ats-dev

# Create API key secrets
kubectl create secret generic market-data-secrets \
  --from-literal=polygon-api-key=your_polygon_key \
  --from-literal=tiingo-api-key=your_tiingo_key \
  --from-literal=alphavantage-api-key=your_av_key \
  -n ats-dev

# Rotate secrets
kubectl delete secret db-credentials -n ats-dev
kubectl create secret generic db-credentials \
  --from-literal=username=postgres \
  --from-literal=password=new_secure_password \
  --from-literal=url="postgresql://postgres:new_secure_password@postgres:5432/dev_db" \
  -n ats-dev

# Restart deployments to pick up new secrets
kubectl rollout restart deployment/analytics-api -n ats-dev
```

### **RBAC Configuration**
```yaml
# security/rbac-config.yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  namespace: ats-dev
  name: ats-developer
rules:
- apiGroups: [""]
  resources: ["pods", "pods/log", "services", "configmaps"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["apps"]
  resources: ["deployments", "replicasets"]
  verbs: ["get", "list", "watch", "patch", "update"]
- apiGroups: ["batch"]
  resources: ["jobs", "cronjobs"]
  verbs: ["get", "list", "watch", "create", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ats-developer-binding
  namespace: ats-dev
subjects:
- kind: User
  name: developer@company.com
  apiGroup: rbac.authorization.k8s.io
roleRef:
  kind: Role
  name: ats-developer
  apiGroup: rbac.authorization.k8s.io
```

---

## 💾 Backup & Recovery

### **Database Backup Strategy**
```bash
# Daily automated backup script
#!/bin/bash
# scripts/backup/daily-db-backup.sh

BACKUP_DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backups/postgresql"
RETENTION_DAYS=30

# Create backup
kubectl exec -n ats-dev deployment/postgres -- pg_dump \
  -U postgres \
  -h localhost \
  -d dev_db \
  --format=custom \
  --compress=9 > "${BACKUP_DIR}/dev_db_${BACKUP_DATE}.dump"

# Verify backup integrity
kubectl exec -n ats-dev deployment/postgres -- pg_restore \
  --list "${BACKUP_DIR}/dev_db_${BACKUP_DATE}.dump" > /dev/null

if [ $? -eq 0 ]; then
  echo "✅ Backup created successfully: dev_db_${BACKUP_DATE}.dump"
else
  echo "❌ Backup verification failed"
  exit 1
fi

# Cleanup old backups
find "${BACKUP_DIR}" -name "dev_db_*.dump" -mtime +${RETENTION_DAYS} -delete

# Upload to cloud storage (if configured)
if [ -n "$AWS_S3_BUCKET" ]; then
  aws s3 cp "${BACKUP_DIR}/dev_db_${BACKUP_DATE}.dump" \
    "s3://${AWS_S3_BUCKET}/database-backups/"
fi
```

### **Disaster Recovery Procedures**
```bash
# Database recovery from backup
#!/bin/bash
# scripts/recovery/restore-database.sh

BACKUP_FILE="$1"
TARGET_DB="$2"

if [ -z "$BACKUP_FILE" ] || [ -z "$TARGET_DB" ]; then
  echo "Usage: ./restore-database.sh <backup_file> <target_database>"
  exit 1
fi

echo "🔄 Starting database recovery..."

# Stop applications
kubectl scale deployment analytics-api --replicas=0 -n ats-dev
kubectl scale deployment ml-inference-service --replicas=0 -n ats-dev

# Drop and recreate database
kubectl exec -n ats-dev deployment/postgres -- psql -U postgres -c "DROP DATABASE IF EXISTS ${TARGET_DB};"
kubectl exec -n ats-dev deployment/postgres -- psql -U postgres -c "CREATE DATABASE ${TARGET_DB};"

# Restore from backup
kubectl exec -n ats-dev deployment/postgres -- pg_restore \
  -U postgres \
  -d "${TARGET_DB}" \
  --verbose \
  --clean \
  --if-exists \
  "${BACKUP_FILE}"

# Restart applications
kubectl scale deployment analytics-api --replicas=3 -n ats-dev
kubectl scale deployment ml-inference-service --replicas=2 -n ats-dev

echo "✅ Database recovery completed"
```

---

## 🔧 Infrastructure Management

### **Capacity Planning**
```python
class CapacityMonitor:
    def analyze_resource_utilization(self, namespace: str = "ats-dev") -> CapacityReport:
        """
        Analyze current resource utilization and forecast needs
        """
        # Get current resource usage
        pods = self.k8s_client.list_namespaced_pod(namespace)
        
        cpu_usage = []
        memory_usage = []
        storage_usage = []
        
        for pod in pods.items:
            metrics = self.metrics_client.get_pod_metrics(pod.metadata.name, namespace)
            cpu_usage.append(metrics['cpu'])
            memory_usage.append(metrics['memory']) 
            storage_usage.append(metrics['storage'])
        
        # Calculate utilization percentages
        cluster_capacity = self.get_cluster_capacity()
        
        current_utilization = {
            'cpu': sum(cpu_usage) / cluster_capacity['cpu'],
            'memory': sum(memory_usage) / cluster_capacity['memory'],
            'storage': sum(storage_usage) / cluster_capacity['storage']
        }
        
        # Forecast future needs based on growth trend
        growth_trend = self.calculate_growth_trend(namespace, days=30)
        
        projected_needs = {
            'cpu': current_utilization['cpu'] * (1 + growth_trend['cpu']),
            'memory': current_utilization['memory'] * (1 + growth_trend['memory']),
            'storage': current_utilization['storage'] * (1 + growth_trend['storage'])
        }
        
        # Generate recommendations
        recommendations = []
        if projected_needs['cpu'] > 0.8:
            recommendations.append("Consider adding CPU capacity within 30 days")
        if projected_needs['memory'] > 0.8:
            recommendations.append("Consider adding memory capacity within 30 days")
        if projected_needs['storage'] > 0.8:
            recommendations.append("Consider adding storage capacity within 30 days")
            
        return CapacityReport(
            current_utilization=current_utilization,
            projected_needs=projected_needs,
            recommendations=recommendations,
            forecast_horizon_days=30
        )
```

### **Performance Optimization**
```bash
# Database performance tuning
kubectl exec -n ats-dev deployment/postgres -- psql -U postgres -d dev_db -c "
-- Update PostgreSQL configuration for time-series workloads
ALTER SYSTEM SET shared_buffers = '2GB';
ALTER SYSTEM SET effective_cache_size = '6GB'; 
ALTER SYSTEM SET work_mem = '256MB';
ALTER SYSTEM SET maintenance_work_mem = '512MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '64MB';
ALTER SYSTEM SET random_page_cost = 1.1;

-- Reload configuration
SELECT pg_reload_conf();
"

# Optimize TimescaleDB settings
kubectl exec -n ats-dev deployment/postgres -- psql -U postgres -d dev_db -c "
-- Set TimescaleDB chunk time intervals
SELECT set_chunk_time_interval('dev_daily_prices', interval '1 month');
SELECT set_chunk_time_interval('dev_minute_prices', interval '1 day');

-- Enable compression on older chunks
SELECT add_compression_policy('dev_daily_prices', interval '7 days');
SELECT add_compression_policy('dev_minute_prices', interval '2 days');
"
```

---

## 🆘 Comprehensive Troubleshooting

### **Common Issues & Solutions**

#### **Database Connection Issues**
```bash
# Symptom: Applications can't connect to database
# Diagnosis:
kubectl get pods -n ats-dev -l app=postgres
kubectl describe pod -n ats-dev -l app=postgres
kubectl logs -n ats-dev deployment/postgres --tail=50

# Solutions:
# 1. Check if postgres pod is running
kubectl get pods -n ats-dev -l app=postgres

# 2. Verify service is accessible
kubectl get service postgres -n ats-dev
kubectl exec -n ats-dev deployment/postgres -- pg_isready -U postgres

# 3. Test connection from application pod
kubectl exec -n ats-dev deployment/analytics-api -- nc -zv postgres 5432

# 4. Check secrets are mounted correctly
kubectl exec -n ats-dev deployment/analytics-api -- env | grep DB

# 5. Restart database if needed
kubectl rollout restart deployment/postgres -n ats-dev
```

#### **High Memory Usage**
```bash
# Symptom: Pods getting OOMKilled or high memory alerts
# Diagnosis:
kubectl top pods -n ats-dev --sort-by=memory
kubectl describe pod <pod-name> -n ats-dev

# Solutions:
# 1. Increase memory limits
kubectl patch deployment analytics-api -n ats-dev -p '{"spec":{"template":{"spec":{"containers":[{"name":"analytics-api","resources":{"limits":{"memory":"4Gi"}}}]}}}}'

# 2. Optimize application memory usage
kubectl exec -n ats-dev deployment/analytics-api -- python -c "
import psutil
process = psutil.Process()
print(f'Memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB')
print(f'Memory percent: {process.memory_percent():.2f}%')
"

# 3. Enable memory profiling
kubectl set env deployment/analytics-api PYTHONMALLOC=debug -n ats-dev
```

#### **ML Model Inference Failures**
```bash
# Symptom: ML predictions returning errors or timeouts
# Diagnosis:
kubectl logs -f deployment/ml-inference-service -n ats-dev --tail=100
curl http://ml-inference-service/health

# Solutions:
# 1. Check model registry connectivity
kubectl exec -n ats-dev deployment/ml-inference-service -- curl -f http://model-registry:5000/health

# 2. Verify model files exist
kubectl exec -n ats-dev deployment/ml-inference-service -- ls -la /models/

# 3. Test model loading
kubectl exec -n ats-dev deployment/ml-inference-service -- python -c "
import joblib
model = joblib.load('/models/support_resistance_v1.2.3.pkl')
print(f'Model loaded successfully: {type(model)}')
"

# 4. Scale inference service
kubectl scale deployment ml-inference-service --replicas=5 -n ats-dev
```

#### **Data Quality Issues**
```bash
# Symptom: Data validation failures or inconsistent data
# Diagnosis:
python scripts/run_dev.py query --query "
SELECT vendor, COUNT(*) as record_count, 
       AVG(CASE WHEN close > 0 THEN 1 ELSE 0 END) as valid_price_ratio
FROM dev_daily_prices 
WHERE date = CURRENT_DATE - 1 
GROUP BY vendor
"

# Solutions:
# 1. Run data quality validation job
python scripts/run_dev.py deploy --file k8s/data-quality-validation-job.yaml

# 2. Check vendor API status
python scripts/run_dev.py deploy --file k8s/vendor-health-check-job.yaml

# 3. Reconcile cross-vendor data
python scripts/run_dev.py deploy --file k8s/cross-vendor-reconciliation-job.yaml
```

### **Emergency Response Procedures**
```bash
# CRITICAL: Production system down
#!/bin/bash
# scripts/emergency/production-incident-response.sh

echo "🚨 PRODUCTION INCIDENT RESPONSE INITIATED"

# 1. Assess impact
kubectl get pods -n ats-prod
kubectl get services -n ats-prod
kubectl top nodes

# 2. Check service health
for service in analytics-api ml-inference-service postgres; do
  echo "Checking $service..."
  kubectl get pods -n ats-prod -l app=$service
  kubectl logs -n ats-prod deployment/$service --tail=20
done

# 3. Check recent deployments
kubectl rollout history deployment/analytics-api -n ats-prod
kubectl rollout history deployment/ml-inference-service -n ats-prod

# 4. Rollback if recent deployment caused issue
read -p "Rollback to previous version? (y/n): " rollback
if [ "$rollback" = "y" ]; then
  kubectl rollout undo deployment/analytics-api -n ats-prod
  kubectl rollout undo deployment/ml-inference-service -n ats-prod
fi

# 5. Notify stakeholders
echo "📧 Sending incident notification..."
curl -X POST https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK \
  -H 'Content-type: application/json' \
  --data '{"text":"🚨 PRODUCTION INCIDENT: ATS Platform experiencing issues. Response team activated."}'

echo "✅ Initial response complete. Monitoring recovery..."
```

---

## 📈 Performance Monitoring

### **Key Performance Indicators**
```python
class PerformanceMonitor:
    def generate_performance_report(self) -> PerformanceReport:
        """
        Generate comprehensive performance report
        """
        return PerformanceReport(
            timestamp=datetime.utcnow(),
            infrastructure_metrics={
                'cluster_cpu_utilization': self.get_cluster_cpu_usage(),
                'cluster_memory_utilization': self.get_cluster_memory_usage(),
                'cluster_storage_utilization': self.get_cluster_storage_usage(),
                'node_availability': self.get_node_availability()
            },
            application_metrics={
                'api_response_time_p95': self.get_api_latency_p95(),
                'api_error_rate': self.get_api_error_rate(),
                'ml_inference_latency_p95': self.get_ml_latency_p95(),
                'database_connection_pool_usage': self.get_db_pool_usage()
            },
            business_metrics={
                'portfolio_update_frequency': self.get_portfolio_update_rate(),
                'data_freshness': self.get_data_freshness_metrics(),
                'prediction_accuracy': self.get_prediction_accuracy(),
                'system_uptime': self.get_system_uptime()
            },
            alerts_summary={
                'critical_alerts_24h': self.get_critical_alerts(hours=24),
                'warning_alerts_24h': self.get_warning_alerts(hours=24),
                'resolved_incidents_24h': self.get_resolved_incidents(hours=24)
            }
        )
```

---

## 🎯 Operational Success Metrics

### **Service Level Objectives (SLOs)**
- **API Availability**: 99.9% uptime (8.77 hours downtime/year)
- **API Response Time**: 95th percentile < 100ms
- **Database Availability**: 99.95% uptime (4.38 hours downtime/year)
- **Data Freshness**: Market data < 5 minutes old during trading hours
- **ML Inference Latency**: 95th percentile < 50ms
- **Backup Success Rate**: 100% daily backups completed successfully

### **Operational Efficiency Metrics**
- **Mean Time to Detection (MTTD)**: < 2 minutes for critical issues
- **Mean Time to Resolution (MTTR)**: < 15 minutes for critical issues
- **Deployment Frequency**: Daily deployments to dev, weekly to production
- **Change Failure Rate**: < 5% of deployments require rollback
- **Alert Fatigue**: < 10 false positive alerts per week

---

**🎯 This operations guide ensures reliable, monitored, and secure operation of the ATS platform with comprehensive troubleshooting procedures and performance optimization strategies.**