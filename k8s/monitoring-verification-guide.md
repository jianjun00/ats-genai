# 🔍 Monitoring Verification Guide

## Step 1: Access Grafana Dashboard

### Get Grafana URL:
```bash
# Method 1: Get NodePort URL
minikube service grafana -n monitoring --url

# Method 2: Port Forward (if NodePort doesn't work)
kubectl port-forward service/grafana 3000:3000 -n monitoring
# Then access: http://localhost:3000
```

### Login Credentials:
- **Username:** `admin`
- **Password:** `admin123`

## Step 2: Verify Dashboard Loading

### Check Available Dashboards:
1. Login to Grafana
2. Click on **"Dashboards"** in the left sidebar
3. Look for **"Monitoring"** folder
4. Verify these dashboards exist:
   - **Kubernetes Cluster Monitoring**
   - **PostgreSQL Database Monitoring**

### If Dashboards Don't Appear:
```bash
# Check Grafana logs for errors
kubectl logs deployment/grafana -n monitoring --tail=20

# Restart Grafana to reload dashboards
kubectl rollout restart deployment/grafana -n monitoring
```

## Step 3: Verify Kubernetes Monitoring

### Access Kubernetes Dashboard:
1. Go to **"Kubernetes Cluster Monitoring"** dashboard
2. Verify these metrics show data:

#### Cluster Overview Section:
- **Total Nodes**: Should show number > 0
- **Running Pods**: Should show active pods
- **Failed Pods**: Should be 0 or low number
- **Active Jobs**: Should show current job count
- **Cluster CPU Usage**: Should show percentage
- **Cluster Memory Usage**: Should show percentage

#### Expected Values:
```bash
# Verify actual cluster state matches dashboard
kubectl get nodes
kubectl get pods --all-namespaces | grep -c Running
kubectl get jobs --all-namespaces
```

## Step 4: Verify PostgreSQL Monitoring

### Access PostgreSQL Dashboard:
1. Go to **"PostgreSQL Database Monitoring"** dashboard
2. Verify these metrics show data:

#### Database Health Section:
- **Database Status**: Should show "UP" (green)
- **Active Connections**: Should show number > 0
- **Cache Hit Ratio**: Should be > 0.8 (80%)
- **Total Tuples**: Should show large numbers
- **Database Size**: Should show size in bytes
- **Active Queries**: Should show current query count

#### Test Database Connection:
```bash
# Verify postgres-exporter is working
kubectl logs deployment/postgres-exporter -n monitoring --tail=10

# Test direct database connection
kubectl exec -it deployment/postgres-exporter -n monitoring -- /bin/sh
# Inside container: curl http://localhost:9187/metrics | grep pg_up
```

## Step 5: Verify Metrics Collection

### Check Prometheus Targets:
```bash
# Port forward to Prometheus
kubectl port-forward service/prometheus 9090:9090 -n monitoring

# Access Prometheus at: http://localhost:9090
# Go to: Status > Targets
# Verify all targets are "UP":
# - postgres-exporter
# - kube-state-metrics
# - node-exporter
```

### Test Specific Metrics:
```bash
# In Prometheus Query interface, test these queries:

# Kubernetes metrics:
count(kube_node_info)
count(kube_pod_info{phase="Running"})

# PostgreSQL metrics:
pg_up
pg_stat_database_numbackends{datname="dev_db"}
```

## Step 6: Verify Data Sources

### Check Grafana Data Sources:
1. In Grafana, go to **Configuration > Data Sources**
2. Verify **Prometheus** data source exists
3. Click **"Test"** button - should show green "Data source is working"

### Test Queries in Dashboard:
1. Open any dashboard panel
2. Click **"Edit"** button
3. In Query tab, click **"Run Query"**
4. Verify data appears in preview

## Step 7: Verify Alerting (Optional)

### Check Alertmanager:
```bash
# Port forward to Alertmanager
kubectl port-forward service/alertmanager 9093:9093 -n monitoring

# Access Alertmanager at: http://localhost:9093
# Check for any active alerts
```

### Test Alert Rules in Prometheus:
```bash
# In Prometheus UI, go to: Alerts
# Verify alert rules are loaded
# Check for any firing alerts
```

## Step 8: Verification Checklist

### ✅ Dashboard Verification:
- [ ] Grafana accessible via URL
- [ ] Login successful with admin/admin123
- [ ] "Monitoring" folder visible
- [ ] Kubernetes dashboard loads with data
- [ ] PostgreSQL dashboard loads with data
- [ ] All panels show metrics (not "No data")

### ✅ Metrics Verification:
- [ ] Node metrics showing (CPU, Memory, Disk)
- [ ] Pod metrics showing (Running, Failed counts)
- [ ] Job metrics showing (Active, Succeeded, Failed)
- [ ] Database connection metrics
- [ ] Database performance metrics (cache hit ratio > 80%)
- [ ] Transaction rates showing

### ✅ Infrastructure Verification:
- [ ] All monitoring pods running
- [ ] Prometheus targets all "UP"
- [ ] No error logs in monitoring components
- [ ] Data sources testing successful

## Step 9: Troubleshooting Common Issues

### Dashboard Shows "No Data":
```bash
# Check if metrics are being scraped
kubectl logs deployment/prometheus -n monitoring | grep -i error

# Verify target endpoints
kubectl get endpoints -n monitoring
```

### PostgreSQL Metrics Missing:
```bash
# Check postgres-exporter connectivity
kubectl logs deployment/postgres-exporter -n monitoring

# Test database connection from exporter
kubectl exec -it deployment/postgres-exporter -n monitoring -- /bin/sh
# Inside: psql "postgresql://postgres:dev_password@postgres-simple.ats-dev:5432/dev_db" -c "SELECT 1"
```

### Kubernetes Metrics Missing:
```bash
# Check kube-state-metrics
kubectl logs deployment/kube-state-metrics -n monitoring

# Verify RBAC permissions
kubectl auth can-i list pods --as=system:serviceaccount:monitoring:kube-state-metrics
```

## Step 10: Performance Validation

### Expected Dashboard Response Times:
- Dashboard load: < 3 seconds
- Panel refresh: < 2 seconds
- Query execution: < 1 second

### Sample Metrics to Verify:
```bash
# These should return data in Prometheus:
up{job="postgres-exporter"}
up{job="kube-state-metrics"}
up{job="node-exporter"}
```

## 🎯 Success Criteria

Your monitoring setup is working correctly if:
1. **All dashboards load without errors**
2. **Metrics show real-time data**
3. **No "No data" messages in panels**
4. **Prometheus targets all show "UP" status**
5. **Database metrics reflect actual database state**
6. **Kubernetes metrics match actual cluster state**

## 📞 Quick Health Check Commands

```bash
# One-liner to verify all monitoring components
kubectl get pods -n monitoring | grep -E "(Running|1/1)"

# Check if all services have endpoints
kubectl get endpoints -n monitoring

# Verify Grafana is accessible
curl -s http://$(minikube ip):$(kubectl get svc grafana -n monitoring -o jsonpath='{.spec.ports[0].nodePort}')/api/health

# Test Prometheus metrics endpoint
kubectl port-forward service/prometheus 9090:9090 -n monitoring &
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health: .health}'
```

If all verification steps pass, your detailed monitoring setup for Kubernetes and PostgreSQL is fully operational! 🎉