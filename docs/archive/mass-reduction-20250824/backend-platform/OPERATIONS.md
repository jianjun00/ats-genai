# ⚙️ Backend Platform Operations

**Deployment, Monitoring, and Troubleshooting Guide**

---

## 🚀 Deployment Procedures

### **Service Deployment Order**
```bash
# 1. Deploy core infrastructure dependencies
kubectl apply -f k8s/redis/
kubectl apply -f k8s/postgres/

# 2. Deploy authentication service (required by all others)
kubectl apply -f k8s/auth-service/
kubectl wait --for=condition=available deployment/auth-service -n ats-dev

# 3. Deploy data access services
kubectl apply -f k8s/market-data-service/
kubectl apply -f k8s/portfolio-service/

# 4. Deploy business logic services  
kubectl apply -f k8s/analytics-service/
kubectl apply -f k8s/signal-service/

# 5. Deploy API Gateway (requires all services)
kubectl apply -f k8s/api-gateway/
```

### **Health Check Verification**
```bash
# Verify all services are healthy
./scripts/verify_backend_health.sh

# Test critical API endpoints
curl -s http://external-ip:port/api/v1/health | jq
curl -s http://external-ip:port/api/v1/portfolio/positions -H "Authorization: Bearer $TOKEN"
```

### **Rolling Updates**
```bash
# Update service with zero downtime
kubectl set image deployment/analytics-service \
  analytics-service=dragonflyer762/ats-genai:v1.2.3 -n ats-dev

# Monitor rollout
kubectl rollout status deployment/analytics-service -n ats-dev

# Verify new version
kubectl logs deployment/analytics-service -n ats-dev | grep "version"
```

---

## 📊 Monitoring & Alerting

### **Key Service Metrics**

#### **API Gateway Metrics**
```yaml
metrics:
  request_rate:
    target: "< 10000 req/sec"
    alert: "> 12000 req/sec"
  
  response_time:
    target: "< 100ms (95th percentile)"
    alert: "> 200ms (95th percentile)"
    
  error_rate:
    target: "< 0.1%"
    alert: "> 1% over 5 minutes"
    
  authentication_failures:
    target: "< 1%"  
    alert: "> 5% over 2 minutes"
```

#### **Service-Specific Metrics**
```yaml
analytics_service:
  portfolio_calculation_time:
    target: "< 500ms"
    alert: "> 2000ms"
    
  cache_hit_rate:
    target: "> 90%"
    alert: "< 70%"

portfolio_service:
  position_sync_lag:
    target: "< 30 seconds"
    alert: "> 2 minutes"
    
  database_connection_pool:
    target: "< 70% utilized"
    alert: "> 90% utilized"
```

### **Monitoring Dashboards**

#### **Service Health Dashboard**
```bash
# Access Grafana dashboard
kubectl port-forward service/grafana 3000:3000 -n monitoring
# Navigate to: http://localhost:3000/d/backend-platform-overview
```

**Key Panels:**
- Request rate and response time trends
- Error rate by service and endpoint  
- Database connection pool utilization
- Cache hit rates and memory usage
- Service dependency health map

#### **Business Metrics Dashboard**  
**Key Panels:**
- Active user sessions
- Portfolio calculation requests
- API usage by client type
- Revenue-generating API calls
- Peak hour capacity utilization

---

## 🔧 Troubleshooting Guide

### **Common Issues & Solutions**

#### **High API Response Times**
**Symptoms:**
- Response time > 200ms consistently
- User complaints about slow loading

**Diagnosis:**
```bash
# Check service resource usage
kubectl top pods -n ats-dev -l app=analytics-service

# Check database performance
kubectl logs deployment/postgres -n ats-dev | grep "slow query"

# Check cache performance
redis-cli info stats | grep cache_hit_ratio
```

**Solutions:**
```bash
# Scale up service replicas
kubectl scale deployment/analytics-service --replicas=5 -n ats-dev

# Increase resource limits
kubectl patch deployment analytics-service -n ats-dev -p \
  '{"spec":{"template":{"spec":{"containers":[{"name":"analytics-service","resources":{"limits":{"memory":"2Gi","cpu":"1000m"}}}]}}}}'

# Clear cache if hit rate is low
redis-cli FLUSHALL
```

#### **Authentication Service Failures**
**Symptoms:**
- Login failures across all clients
- 401/403 errors on authenticated endpoints

**Diagnosis:**
```bash
# Check auth service status
kubectl get pods -n ats-dev -l app=auth-service
kubectl logs deployment/auth-service -n ats-dev --tail=50

# Check JWT token validation
kubectl exec -it deployment/auth-service -n ats-dev -- python -c \
  "import jwt; print(jwt.decode('$TOKEN', verify=False))"
```

**Solutions:**
```bash
# Restart auth service
kubectl rollout restart deployment/auth-service -n ats-dev

# Check JWT secret configuration
kubectl get secret jwt-secret -n ats-dev -o yaml

# Verify database connectivity
kubectl exec -it deployment/auth-service -n ats-dev -- \
  python -c "import psycopg2; conn = psycopg2.connect('$DB_URL'); print('OK')"
```

#### **Database Connection Pool Exhaustion**
**Symptoms:**
- "connection pool exhausted" errors
- New requests timing out
- High database connection count

**Diagnosis:**
```bash
# Check PostgreSQL connections
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c \
  "SELECT count(*) FROM pg_stat_activity WHERE state = 'active';"

# Check service connection pool status
kubectl logs deployment/portfolio-service -n ats-dev | grep "connection pool"
```

**Solutions:**
```bash
# Increase connection pool size
kubectl patch deployment portfolio-service -n ats-dev -p \
  '{"spec":{"template":{"spec":{"containers":[{"name":"portfolio-service","env":[{"name":"DB_POOL_SIZE","value":"20"}]}]}}}}'

# Kill idle connections
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c \
  "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = 'idle' AND state_change < now() - interval '5 minutes';"
```

---

## 🔍 Performance Optimization

### **Database Query Optimization**
```sql
-- Identify slow queries
SELECT query, mean_time, calls 
FROM pg_stat_statements 
ORDER BY mean_time DESC 
LIMIT 10;

-- Add missing indexes
CREATE INDEX CONCURRENTLY idx_positions_portfolio_symbol 
ON positions (portfolio_id, symbol, updated_at DESC);

-- Analyze query plans
EXPLAIN ANALYZE 
SELECT p.symbol, p.quantity, p.market_value 
FROM positions p 
WHERE p.portfolio_id = 123 
  AND p.updated_at > NOW() - INTERVAL '1 day';
```

### **Caching Optimization**
```python
# Redis cache configuration
CACHE_CONFIG = {
    'portfolio_positions': {'ttl': 300},      # 5 minutes
    'market_prices': {'ttl': 60},             # 1 minute  
    'user_sessions': {'ttl': 3600},           # 1 hour
    'analytics_results': {'ttl': 900},        # 15 minutes
}

# Cache warming strategy
async def warm_cache():
    """Pre-load frequently accessed data"""
    active_portfolios = await get_active_portfolios()
    for portfolio_id in active_portfolios:
        await cache_portfolio_positions(portfolio_id)
```

### **Service Scaling Strategies**
```yaml
# Horizontal Pod Autoscaler
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: analytics-service-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: analytics-service
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource  
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

---

## 🔒 Security Operations

### **Security Monitoring**
```bash
# Monitor authentication failures
kubectl logs deployment/auth-service -n ats-dev | \
  grep "authentication failed" | tail -20

# Check for suspicious API access patterns  
kubectl logs deployment/api-gateway -n ats-dev | \
  grep -E "(429|401|403)" | \
  awk '{print $1}' | sort | uniq -c | sort -nr

# Verify JWT token expiration
kubectl exec -it deployment/auth-service -n ats-dev -- \
  python -c "
import jwt, datetime
token = '$SUSPICIOUS_TOKEN'
decoded = jwt.decode(token, verify=False)
print(f'Expires: {datetime.datetime.fromtimestamp(decoded[\"exp\"])}')
"
```

### **Security Incident Response**
```bash
# Block suspicious IP addresses
kubectl create -f - <<EOF
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: block-suspicious-ips
spec:
  podSelector:
    matchLabels:
      app: api-gateway
  policyTypes:
  - Ingress
  ingress:
  - from: []
    except:
    - ipBlock:
        cidr: 192.168.1.100/32  # Suspicious IP
EOF

# Rotate JWT secrets
kubectl create secret generic jwt-secret-new \
  --from-literal=JWT_SECRET=$(openssl rand -base64 32) -n ats-dev

# Update service to use new secret
kubectl patch deployment auth-service -n ats-dev -p \
  '{"spec":{"template":{"spec":{"containers":[{"name":"auth-service","envFrom":[{"secretRef":{"name":"jwt-secret-new"}}]}]}}}}'
```

---

## 💾 Backup & Recovery

### **Database Backup Strategy**
```bash
# Daily automated backup
kubectl create job --from=cronjob/postgres-backup postgres-backup-manual -n ats-dev

# Verify backup integrity
kubectl logs job/postgres-backup-manual -n ats-dev

# Restore from backup (EMERGENCY ONLY)
kubectl exec -it deployment/postgres -n ats-dev -- \
  pg_restore -U postgres -d dev_db /backups/postgres_backup_20240115.sql
```

### **Configuration Backup**
```bash
# Backup all K8s configurations
kubectl get all -n ats-dev -o yaml > backend-platform-backup-$(date +%Y%m%d).yaml

# Backup secrets (encrypted)
kubectl get secrets -n ats-dev -o yaml | \
  gpg --encrypt --recipient admin@company.com > secrets-backup-$(date +%Y%m%d).yaml.gpg
```

---

## 🎯 Operational Runbooks

### **Service Restart Procedure**
```bash
#!/bin/bash
# restart_backend_service.sh

SERVICE_NAME=$1
NAMESPACE=${2:-ats-dev}

echo "Restarting $SERVICE_NAME in $NAMESPACE..."

# 1. Check current service health
kubectl get deployment/$SERVICE_NAME -n $NAMESPACE

# 2. Restart with rolling update
kubectl rollout restart deployment/$SERVICE_NAME -n $NAMESPACE

# 3. Wait for rollout to complete
kubectl rollout status deployment/$SERVICE_NAME -n $NAMESPACE --timeout=300s

# 4. Verify service health
kubectl get pods -n $NAMESPACE -l app=$SERVICE_NAME
./scripts/verify_backend_health.sh

echo "Service restart completed successfully"
```

### **Emergency Scale-Up Procedure**
```bash
#!/bin/bash
# emergency_scale.sh

# Scale critical services for high traffic
kubectl scale deployment/api-gateway --replicas=8 -n ats-dev
kubectl scale deployment/analytics-service --replicas=6 -n ats-dev  
kubectl scale deployment/portfolio-service --replicas=4 -n ats-dev

# Monitor scaling
watch 'kubectl get pods -n ats-dev | grep -E "(api-gateway|analytics|portfolio)"'

# Verify increased capacity
./scripts/load_test.sh --duration=60s --rps=5000
```

---

**🎯 This operations guide ensures reliable, monitored, and scalable backend platform operations with comprehensive troubleshooting procedures.**