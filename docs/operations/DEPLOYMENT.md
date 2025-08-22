# 🚀 ATS-GenAI Deployment Guide

## 📋 **Environment Configuration Summary**

### **🔧 Development Environment (dev)**

#### **Database Configuration:**
- **Host**: `postgres` (internal k8s) / `localhost` (external)
- **Port**: `5432`
- **Username**: `postgres`
- **Password**: `dev_password`
- **Database**: `dev_db`
- **Connection String**: `postgresql://postgres:dev_password@postgres:5432/dev_db?sslmode=disable`

#### **Kubernetes Configuration:**
- **Namespace**: `ats-dev`
- **Cluster Type**: Minikube
- **External IP**: `192.168.49.2`
- **Secrets**:
  - `db-credentials-dev`: Contains `DB_USER`, `DB_PASSWORD`, `DB_NAME`
  - `api-keys`: Contains `polygon-api-key`

#### **Analytics API Access:**
- **Pod Status**: ✅ **VERIFIED RUNNING**
- **Internal URL**: `http://analytics-api-service.ats-dev.svc.cluster.local:8000`
- **External URL (NodePort)**: `http://192.168.49.2:30800`
- **Port Forward Access**: `kubectl port-forward svc/analytics-api-service 8000:8000 -n ats-dev`
- **Local Access**: `http://localhost:8000`
- **Response Verified**: `{"message":"ATS GenAI API is running"}` ✅

#### **Dynamic Analytics API (For Rapid Python Changes):**
- **Deployment**: `dynamic-analytics-api` in `ats-dev` namespace
- **Internal URL**: `http://dynamic-analytics-service.ats-dev.svc.cluster.local:8000`
- **External URL (NodePort)**: `http://192.168.49.2:30801`
- **Port Forward Access**: `kubectl port-forward svc/dynamic-analytics-service 8002:8000 -n ats-dev`
- **Local Access**: `http://localhost:8002`
- **Module Path**: `src.analytics_api_dynamic:app`
- **Purpose**: Rapid deployment of Python changes without Docker rebuilds

#### **Services Running:**
- **postgres**: `postgres.ats-dev.svc.cluster.local:5432`
- **timescaledb**: `timescaledb.ats-dev.svc.cluster.local:5432` 
- **analytics-api-service**: `analytics-api-service.ats-dev.svc.cluster.local:8000`

---

### **🧪 Integration Environment (intg)**

#### **Database Configuration:**
- **Host**: `postgres-intg` (internal k8s) / `localhost` (external)
- **Port**: `5432`
- **Username**: `postgres`
- **Password**: `intg_password` *(needs verification)*
- **Database**: `intg_db`
- **Connection String**: `postgresql://postgres:intg_password@postgres-intg:5432/intg_db?sslmode=disable`

#### **Kubernetes Configuration:**
- **Namespace**: `ats-intg`
- **Cluster Type**: Production-like
- **External IP**: *TBD - depends on cloud provider*
- **Secrets**: 
  - `db-credentials-intg`: Contains database credentials
  - `api-keys-intg`: Contains API keys for integration testing

#### **Analytics API Access:**
- **Internal URL**: `http://analytics-api-service.ats-intg.svc.cluster.local:8000`
- **External URL**: *TBD - LoadBalancer/Ingress configuration*
- **Port**: `8000`

---

### **🏭 Production Environment (prod)**

#### **Database Configuration:**
- **Host**: `postgres-prod` (internal k8s) / *secure endpoint* (external)
- **Port**: `5432`
- **Username**: `postgres`
- **Password**: `prod_password` *(securely managed)*
- **Database**: `prod_db`
- **Connection String**: `postgresql://postgres:prod_password@postgres-prod:5432/prod_db?sslmode=require`

#### **Kubernetes Configuration:**
- **Namespace**: `ats-prod`
- **Cluster Type**: Production cluster with HA
- **External IP**: *Production LoadBalancer IP*
- **Secrets**: 
  - `db-credentials-prod`: Contains encrypted database credentials
  - `api-keys-prod`: Contains production API keys
  - SSL certificates for secure connections

#### **Analytics API Access:**
- **Internal URL**: `http://analytics-api-service.ats-prod.svc.cluster.local:8000`
- **External URL**: `https://analytics.ats-genai.prod.com` *(example)*
- **Port**: `8000` (internal), `443` (external with SSL)

---

## 🌐 **Verified Working URLs**

### **✅ Development Environment (VERIFIED)**
```bash
# Port Forward Method (VERIFIED WORKING)
kubectl port-forward svc/analytics-api-service 8000:8000 -n ats-dev
curl http://localhost:8000/
# Response: {"message":"ATS GenAI API is running"}

# Direct NodePort Access
http://192.168.49.2:30800/
# Status: ✅ Service deployed, ⚠️ Network access needs verification

# Internal Cluster Access
http://analytics-api-service.ats-dev.svc.cluster.local:8000/
```

### **📊 Available API Endpoints:**
- **Root**: `/` - API status message
- **Health**: `/health` - Service health check *(if implemented)*
- **Events**: `/events/*` - Event system endpoints
- **Status**: `/status` - System status *(if implemented)*

---

## 🔐 **Security Configuration**

### **Database Security:**
- **Development**: `sslmode=disable` (local development)
- **Integration**: `sslmode=prefer` (testing with SSL)
- **Production**: `sslmode=require` (mandatory SSL)

### **API Security:**
- **Development**: HTTP (local testing)
- **Integration**: HTTPS with self-signed certs
- **Production**: HTTPS with valid SSL certificates

### **Secret Management:**
- **Development**: Kubernetes secrets in `ats-dev` namespace
- **Integration**: Encrypted secrets with key rotation
- **Production**: HashiCorp Vault or cloud secret manager

---

## 🔄 **Development vs Kubernetes Deployment**

### **⚠️ IMPORTANT: Choose the Right Deployment Method**

1. **Local Development** (for code development and testing):
   - Use `PYTHONPATH=src python src/analytics_api_dynamic.py` 
   - Requires local database connection
   - For rapid code iteration and debugging

2. **Kubernetes Deployment** (for integration testing and production):
   - Use `kubectl apply -f dynamic_analytics_k8s.yaml`
   - Connects to Kubernetes-hosted database
   - For testing with real infrastructure

3. **🚀 Volume Mount Pattern** (RECOMMENDED for Python development):
   - Mount Python scripts via ConfigMaps or volumes
   - No Docker image rebuilding required
   - Perfect for rapid Python iteration in Kubernetes
   - See examples in `k8s/modeling-universe-with-mount.yaml`

**DO NOT** mix local and Kubernetes approaches - choose one based on your testing needs.

---

## 🚀 **Deployment Commands**

### **Deploy Analytics API to Development:**
```bash
# Apply the working deployment
kubectl apply -f working_analytics_api.yaml

# Verify deployment
kubectl get pods -n ats-dev -l app=ats-analytics
kubectl logs deployment/analytics-api -n ats-dev

# Test access
kubectl port-forward svc/analytics-api-service 8000:8000 -n ats-dev
curl http://localhost:8000/
```

### **Deploy Analytics API to Integration:**
```bash
# Update namespace and secrets for integration
sed 's/ats-dev/ats-intg/g' working_analytics_api.yaml > integration_analytics_api.yaml
sed 's/db-credentials-dev/db-credentials-intg/g' -i integration_analytics_api.yaml

# Deploy to integration
kubectl apply -f integration_analytics_api.yaml

# Verify
kubectl get pods -n ats-intg -l app=ats-analytics
```

### **Deploy Analytics API to Production:**
```bash
# Use production configuration with SSL and security
kubectl apply -f production_analytics_api.yaml -n ats-prod

# Verify with health checks
kubectl get pods -n ats-prod -l app=ats-analytics
kubectl logs deployment/analytics-api -n ats-prod
```

### **🚀 Volume Mount Pattern for Python Development:**
```bash
# Example: Deploy Python script without Docker image rebuild
# 1. Create ConfigMap with your Python script
kubectl create configmap my-python-script --from-file=script.py -n ats-dev

# 2. Deploy job with mounted script
cat << EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: python-job-with-mount
  namespace: ats-dev
spec:
  template:
    spec:
      containers:
      - name: python-runner
        image: python:3.12-slim
        command: ["/bin/bash", "-c"]
        args:
        - |
          pip install asyncpg  # Install required packages
          python /scripts/script.py  # Run mounted script
        env:
        - name: DB_HOST
          value: "postgres"
        # ... database credentials from secrets
        volumeMounts:
        - name: script-volume
          mountPath: /scripts
      volumes:
      - name: script-volume
        configMap:
          name: my-python-script
      restartPolicy: Never
EOF

# 3. Update script and redeploy quickly
kubectl create configmap my-python-script --from-file=script.py -n ats-dev --dry-run=client -o yaml | kubectl apply -f -
```

### **Benefits of Volume Mount Pattern:**
- ✅ **No Docker rebuilds** - Update scripts in seconds
- ✅ **Rapid iteration** - Test changes immediately  
- ✅ **Version control** - Scripts tracked in git
- ✅ **Database access** - Full access to k8s resources
- ✅ **Dependency management** - Install packages as needed

---

## 🧪 **Testing & Verification**

### **Development Testing (VERIFIED ✅):**
```bash
# Test database connectivity
kubectl run test-db --image=dragonflyer762/ats-genai:dev-latest --rm -it --restart=Never -n ats-dev -- python -c "
import asyncpg
import asyncio
async def test():
    conn = await asyncpg.connect('postgresql://postgres:dev_password@postgres:5432/dev_db')
    print('✅ Database connection successful')
    await conn.close()
asyncio.run(test())
"

# Test API connectivity  
kubectl port-forward svc/analytics-api-service 8000:8000 -n ats-dev &
curl http://localhost:8000/
# Expected: {"message":"ATS GenAI API is running"}
```

### **Integration Testing:**
```bash
# Test with integration credentials
# Test SSL connections
# Test load balancing
```

### **Production Testing:**
```bash
# Test production endpoints
# Test SSL certificates
# Test monitoring and alerting
```

---

## 📊 **Monitoring & Observability**

### **Health Check Endpoints:**
- **Database**: Check connection to PostgreSQL/TimescaleDB
- **API**: Check FastAPI application health
- **Kubernetes**: Pod readiness and liveness probes

### **Logging:**
```bash
# View API logs
kubectl logs deployment/analytics-api -n ats-dev

# View database logs
kubectl logs deployment/postgres -n ats-dev

# View all ats-analytics logs
kubectl logs -l app=ats-analytics -n ats-dev
```

### **Metrics & Monitoring:**
- **Prometheus**: Metrics collection
- **Grafana**: Dashboards and visualization
- **Alerting**: Critical system alerts

---

## 🔄 **Environment Promotion**

### **Dev → Integration:**
1. Verify all tests pass in development
2. Update configuration for integration environment
3. Deploy to `ats-intg` namespace
4. Run integration test suite
5. Verify external connectivity

### **Integration → Production:**
1. Complete integration testing
2. Security review and approval
3. Update production configuration
4. Deploy to `ats-prod` namespace
5. Run production smoke tests
6. Monitor for 24 hours

---

## 📝 **Configuration Files**

### **Key Files:**
- `working_analytics_api.yaml` - Development deployment (✅ VERIFIED)
- `integration_analytics_api.yaml` - Integration deployment
- `production_analytics_api.yaml` - Production deployment
- `CLAUDE.md` - Development and testing guidelines

### **Secret Templates:**
```yaml
# db-credentials-dev
apiVersion: v1
kind: Secret
metadata:
  name: db-credentials-dev
  namespace: ats-dev
type: Opaque
data:
  DB_USER: cG9zdGdyZXM=      # postgres
  DB_PASSWORD: ZGV2X3Bhc3N3b3Jk  # dev_password
  DB_NAME: ZGV2X2Ri          # dev_db
```

---

## ✅ **Verification Status**

### **Development Environment:**
- ✅ **Database**: Connection verified with dev_password
- ✅ **API Deployment**: Pod running successfully
- ✅ **Service Access**: Port forward verified working
- ✅ **API Response**: Returns proper JSON response
- ⚠️ **External Access**: NodePort needs network verification
- ✅ **Secret Configuration**: All secrets properly configured

### **Integration Environment:**
- ⚠️ **Database**: Configuration defined, needs verification
- ⚠️ **API Deployment**: Template ready, needs deployment
- ⚠️ **External Access**: Needs LoadBalancer configuration

### **Production Environment:**
- ⚠️ **Database**: Secure configuration defined
- ⚠️ **API Deployment**: Production template needed
- ⚠️ **Security**: SSL and secret management needed

---

## 🎯 **Next Steps**

1. **Complete external access verification** for dev environment
2. **Deploy and verify integration environment**
3. **Create production security configuration**
4. **Set up monitoring and alerting**
5. **Create CI/CD pipeline for automated deployments**

**Current Status**: Development environment ✅ **VERIFIED WORKING** with analytics API accessible at `http://localhost:8000` via port forwarding.