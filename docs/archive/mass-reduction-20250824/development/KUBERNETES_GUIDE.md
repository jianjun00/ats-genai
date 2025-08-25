# Kubernetes Development Guide

## 🚨 Kubernetes-First Development

**ALL development operations use Kubernetes in the `ats-dev` namespace:**

- ✅ **DEV Environment = Kubernetes (ats-dev namespace)**
- ✅ **Database = postgres service in K8s cluster**
- ✅ **All data operations = Use Kubernetes jobs**
- ❌ **NEVER try to run data scripts locally for dev environment**
- ❌ **NEVER use localhost database connections for dev work**

## Dev CLI - Your Primary Interface

### Always Use Dev CLI (Never kubectl directly)

**❌ NEVER use kubectl directly for dev operations**  
**✅ ALWAYS use `run_dev` for dev work**

```bash
# Database queries (most common)
run_dev query "SELECT COUNT(*) FROM dev_daily_prices"
run_dev query "SELECT * FROM dev_instruments WHERE symbol = 'AAPL'"

# Run database migrations
run_dev migrate price-unification

# Run data processing jobs
run_dev job price-unification --symbols AAPL,MSFT --date 2024-01-15

# List current jobs
run_dev list

# Get job logs
run_dev logs job-name

# Check job status
run_dev status job-name
```

### Why Dev CLI Instead of kubectl?

**Dev CLI Benefits:**
- Handles authentication automatically
- Uses correct namespace (ats-dev) by default
- Provides consistent interface across all operations
- Includes error handling and retry logic
- Logs all operations for debugging

**kubectl Problems:**
- Easy to forget namespace (`-n ats-dev`)
- No automatic retry on failures
- Verbose syntax for common operations
- Manual secret/config management

## Environment Configuration

### Pre-Configured Environment Variables

**❌ NEVER manually specify environment variables:**
```bash
# DON'T DO THIS:
PYTHONPATH=src ENVIRONMENT=dev DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=dev_password DB_NAME=dev_db python script.py
```

**✅ Environment variables are automatically configured:**
- All scripts in `scripts/backfill/` work with pre-configured K8s environment
- Flyte workflows automatically inherit environment variables from cluster
- Database connections, API keys, and Python paths are set up in deployment manifests
- ConfigMaps and Secrets handle all configuration

### Database Connections

**Kubernetes (primary):**
```bash
DB_HOST=postgres  # K8s service name
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=dev_password  # From K8s secret
DB_NAME=dev_db
```

**Port-forwarding (local testing only):**
```bash
# Only for local testing - not for dev work
kubectl port-forward -n ats-dev service/postgres 5433:5432
# Then: localhost:5433
```

## Infrastructure Patterns

### Reuse Existing Infrastructure

**❌ NEVER create new deployment patterns when existing ones work**  
**✅ ALWAYS check existing infrastructure first**

```bash
# Scan existing infrastructure
kubectl get all -n ats-dev
kubectl get configmaps -n ats-dev
kubectl get secrets -n ats-dev

# Check successful deployment patterns
kubectl get deployment working-analytics-webapp -n ats-dev -o yaml
kubectl get configmap working-analytics-webapp-config -n ats-dev -o yaml
```

### Copying Successful Patterns

```bash
# Copy existing deployment as template
kubectl get deployment working-webapp -n ats-dev -o yaml > new-webapp-deployment.yaml

# Copy existing ConfigMap
kubectl get configmap working-webapp-config -n ats-dev -o yaml > new-webapp-config.yaml

# Modify minimally and apply
# (edit files with your changes)
kubectl apply -f new-webapp-deployment.yaml
kubectl apply -f new-webapp-config.yaml
```

### ConfigMap Management

**Create ConfigMaps from code files:**
```bash
# Create ConfigMap from Python script
kubectl create configmap new-webapp-config \
  --from-file=webapp.py=src/webapp/new_webapp.py \
  -n ats-dev

# Create ConfigMap from directory
kubectl create configmap job-scripts \
  --from-file=scripts/jobs/ \
  -n ats-dev

# Update existing ConfigMap
kubectl create configmap updated-config \
  --from-file=webapp.py=updated_webapp.py \
  --dry-run=client -o yaml | kubectl apply -f -
```

## Docker & Container Strategy

### Use Base Docker Images (Don't Install Packages)

**❌ NEVER install packages in Kubernetes jobs:**
```yaml
# DON'T DO THIS:
containers:
- name: job
  image: python:3.12-slim
  command: ["bash", "-c", "pip install asyncpg pandas && python script.py"]
```

**✅ Use existing base Docker image with pre-installed packages:**
```yaml
# DO THIS:
containers:
- name: job
  image: your-base-image:latest  # Has all packages pre-installed
  command: ["python", "/app/script.py"]
```

### Flyte Integration

**Use Flyte workflows with dynamic code upload:**
```bash
# Run Flyte workflow (uses base Docker + code upload)
python scripts/flyte/flyte_instrument_polygon_workflow.py \
  --job-type backfill \
  --apply \
  --output-dir k8s/generated
```

**Benefits:**
- Leverages optimized base images with ML/data packages
- Dynamic code upload to pre-configured containers
- No package installation delays or errors
- Consistent environment across all jobs

## Job Management

### Running Data Processing Jobs

```bash
# Run price unification job
run_dev job price-unification \
  --symbols AAPL,MSFT,GOOGL \
  --start-date 2024-01-01 \
  --end-date 2024-01-31

# Run market cap computation
run_dev job market-cap \
  --symbols AAPL,MSFT \
  --debug

# Run training data generation
run_dev job enhanced-training \
  --symbol TSLA \
  --days-back 120
```

### Monitoring Jobs

```bash
# List all running jobs
run_dev list

# Get job logs
run_dev logs price-unification-job-abc123

# Check job status
run_dev status market-cap-job-def456

# Follow job logs in real-time
run_dev logs enhanced-training-job-ghi789 --follow
```

### Job Troubleshooting

```bash
# Check job pod status
run_dev describe job-name

# Get detailed pod information
kubectl describe pod job-pod-name -n ats-dev

# Check job events
kubectl get events -n ats-dev --sort-by=.metadata.creationTimestamp

# Debug failed job
run_dev debug job-name
```

## Service Management

### Deploying Web Applications

```bash
# Deploy analytics webapp using existing pattern
kubectl get configmap working-analytics-webapp-config -n ats-dev -o yaml > base-config.yaml

# Modify base-config.yaml with new features
# Create new ConfigMap
kubectl create configmap enhanced-webapp-config \
  --from-file=webapp.py=enhanced_webapp.py \
  -n ats-dev

# Copy and modify deployment
kubectl get deployment working-analytics-webapp -n ats-dev -o yaml > enhanced-webapp-deployment.yaml
# Edit enhanced-webapp-deployment.yaml to use new ConfigMap

# Apply deployment
kubectl apply -f enhanced-webapp-deployment.yaml
```

### Service Discovery & Access

```bash
# Get service external access information
kubectl get service enhanced-webapp-service -n ats-dev

# Get node IP for external access
kubectl get nodes -o wide

# Test external access (not port-forwarding)
curl -s "http://NODE_IP:NODE_PORT/health"

# Check service endpoints
kubectl get endpoints enhanced-webapp-service -n ats-dev
```

## Development Workflow Integration

### TDD with Kubernetes

```bash
# 1. Write failing test
touch tests/integration/test_k8s_deployment.py

# 2. Test current K8s deployment
PYTHONPATH=src pytest tests/integration/test_k8s_deployment.py -v
# ✅ Should fail initially

# 3. Deploy/fix K8s resources
kubectl apply -f fixed-deployment.yaml

# 4. Test again
PYTHONPATH=src pytest tests/integration/test_k8s_deployment.py -v
# ✅ Should pass now
```

### End-to-End Testing in K8s

```bash
# 1. Deploy complete system
run_dev deploy complete-system

# 2. Generate real data
run_dev job data-generation --symbol AAPL

# 3. Verify data in database  
run_dev query "SELECT COUNT(*) FROM dev_daily_prices WHERE symbol = 'AAPL'"

# 4. Test API endpoints
curl -s "http://external-ip:port/api/data/AAPL" | jq

# 5. Verify web interface
curl -s "http://external-ip:port/" | grep -i "AAPL"
```

## Common Kubernetes Patterns

### Job Templates

**Basic Job Template:**
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: data-processing-job
  namespace: ats-dev
spec:
  template:
    spec:
      containers:
      - name: processor
        image: your-base-image:latest
        env:
        - name: PYTHONPATH
          value: "src"
        - name: DB_HOST
          value: "postgres"
        volumeMounts:
        - name: code
          mountPath: /app
      volumes:
      - name: code
        configMap:
          name: processing-scripts
      restartPolicy: Never
  backoffLimit: 3
```

### Service Templates

**Basic Service Template:**
```yaml
apiVersion: v1
kind: Service
metadata:
  name: webapp-service
  namespace: ats-dev
spec:
  type: NodePort
  ports:
  - port: 8000
    targetPort: 8000
    nodePort: 32090
  selector:
    app: webapp
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: webapp-deployment
  namespace: ats-dev
spec:
  replicas: 1
  selector:
    matchLabels:
      app: webapp
  template:
    metadata:
      labels:
        app: webapp
    spec:
      containers:
      - name: webapp
        image: python:3.12-slim
        ports:
        - containerPort: 8000
        volumeMounts:
        - name: webapp-code
          mountPath: /app
      volumes:
      - name: webapp-code
        configMap:
          name: webapp-config
```

## Troubleshooting Guide

### Common Issues

**1. Pod Not Starting:**
```bash
# Check pod status
kubectl get pods -n ats-dev | grep job-name

# Describe pod for events
kubectl describe pod pod-name -n ats-dev

# Check logs
kubectl logs pod-name -n ats-dev
```

**2. Database Connection Issues:**
```bash
# Test database connectivity from pod
kubectl exec -it pod-name -n ats-dev -- psql -h postgres -U postgres -d dev_db -c "SELECT 1"

# Check database service
kubectl get service postgres -n ats-dev

# Verify database password secret
kubectl get secret postgres-secret -n ats-dev -o yaml
```

**3. ConfigMap Not Updating:**
```bash
# Delete and recreate ConfigMap
kubectl delete configmap webapp-config -n ats-dev
kubectl create configmap webapp-config --from-file=webapp.py=new_webapp.py -n ats-dev

# Restart deployment to pick up changes
kubectl rollout restart deployment webapp-deployment -n ats-dev
```

**4. External Access Not Working:**
```bash
# Check node IP and port
kubectl get nodes -o wide
kubectl get service service-name -n ats-dev

# Test from outside cluster (not port-forward)
curl -v "http://NODE_IP:NODE_PORT/health"

# Check firewall/network policies
kubectl get networkpolicies -n ats-dev
```

## Best Practices Summary

**DO:**
- ✅ Use dev CLI for all operations
- ✅ Reuse existing deployment patterns
- ✅ Use base Docker images with pre-installed packages
- ✅ Test external access, not just port-forwarding
- ✅ Use ConfigMaps for code deployment
- ✅ Monitor jobs with dev CLI
- ✅ Follow TDD with K8s integration tests

**DON'T:**
- ❌ Use kubectl directly for dev work
- ❌ Create new deployment patterns unnecessarily
- ❌ Install packages in job containers
- ❌ Set environment variables manually
- ❌ Test only via port-forwarding
- ❌ Skip integration testing with K8s
- ❌ Assume ConfigMaps update automatically

---

*Remember: When user says "dev environment" → Use dev CLI and K8s, not local development!*