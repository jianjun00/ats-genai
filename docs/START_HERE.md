# 🚀 ATS Platform - START HERE

**Get up and running with the ATS algorithmic trading platform in 15 minutes.**

---

## 📋 What Is ATS?

**ATS is a Kubernetes-first fintech platform** for algorithmic trading:
- **🏗️ Enterprise Architecture**: Market-neutral portfolio optimization, multi-vendor data, Smart Money Zones
- **☸️ Kubernetes-Native**: Everything runs in K8s (ats-dev namespace) 
- **🧪 Test-Driven**: Write failing tests first, then implement
- **🔄 End-to-End**: Real data → Database → API → Frontend → Tests pass

---

## ⚡ 5-Minute Setup

### 1. Clone and Install (2 min)
```bash
git clone https://github.com/your-org/ats-genai
cd ats-genai
uv sync
```

### 2. Verify Dev CLI Access (2 min)
```bash
# Test dev CLI - this is your PRIMARY interface
python3 scripts/run_dev.py psql --query "SELECT 1"
# ✅ Success = you're connected to K8s cluster
# ❌ Fails = ask team for cluster access
```

### 3. Run First Test (1 min)
```bash
PYTHONPATH=src pytest tests/integration/test_analytics_platform_integration.py::TestAnalyticsPlatformIntegration::test_backend_api_can_start -v
# ✅ Pass = environment ready
# ❌ Fail = check troubleshooting below
```

---

## 🎯 Core Concepts (10 minutes)

### ☸️ Kubernetes-First Development

**✅ ALWAYS Use:**
```bash
python3 scripts/run_dev.py psql --query "SELECT COUNT(*) FROM dev_daily_prices"
python3 scripts/run_dev.py deploy --file k8s/your-job.yaml
python3 scripts/run_dev.py logs --job job-name
python3 scripts/run_dev.py status
```

**❌ NEVER Use:**
```bash
kubectl get pods -n ats-dev              # Use run_dev instead
PYTHONPATH=src python script.py          # Use K8s jobs instead
```

### 🧪 Test-Driven Development (TDD)

**MANDATORY sequence for ALL code changes:**
```bash
# 1. Write failing test FIRST
touch tests/integration/test_new_feature.py
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v
# ✅ Should FAIL (proves test works)

# 2. Write minimal code to make test pass
# (implement your feature in src/)

# 3. Verify test passes
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v
# ✅ Should PASS

# 4. Run full test suite
PYTHONPATH=src pytest tests/ -v
```

### 🔄 End-to-End Validation

**Features aren't complete until entire pipeline works:**
1. **Generate real data** in K8s cluster
2. **Store in database** with correct schema  
3. **API serves data** to external clients
4. **Frontend displays data** in browser
5. **All integration tests pass**

---

## 👤 Role-Specific Quick Actions

### 🔧 Backend Engineer
```bash
# Deploy API endpoint
kubectl apply -f k8s/your-service.yaml
# Test external access
curl -s "http://external-ip:port/api/endpoint" | jq
```

### 📊 Data Engineer
```bash
# Deploy data pipeline job
kubectl apply -f k8s/your-data-job.yaml -n ats-dev
# Verify data quality
python3 scripts/run_dev.py psql --query "SELECT COUNT(*) FROM dev_daily_prices WHERE symbol IN ('AAPL', 'MSFT')"
```

### 🎨 Frontend Engineer  
```bash
# Deploy webapp
kubectl apply -f k8s/webapp-deployment.yaml
# Test in browser
curl -s "http://external-ip:port/" | grep "Welcome to ATS"
```

### 🤖 Model Developer
```bash
# Deploy training data job
kubectl apply -f k8s/enhanced-training-job.yaml -n ats-dev
# Verify dataset
python3 scripts/run_dev.py psql --query "SELECT COUNT(*) FROM dev_daily_prices ORDER BY date DESC LIMIT 5"
```

---

## ✅ Verification Commands

**Run these to verify your setup works:**
```bash
# 1. Database connectivity
python3 scripts/run_dev.py psql --query "SELECT version()"

# 2. Job execution capability  
python3 scripts/run_dev.py status

# 3. External service access
kubectl get nodes -o wide
kubectl get service postgres -n ats-dev

# 4. Integration tests
PYTHONPATH=src pytest tests/integration/ -v --tb=short
```

---

## 🆘 Common Issues & Fixes

### "dev CLI not working"
```bash
# Check cluster access
kubectl get pods -n ats-dev
# If fails: ask team for cluster access
# If works: check dev CLI exists at scripts/run_dev.py
```

### "Database connection failed"  
```bash
python3 scripts/run_dev.py psql --query "SELECT 1"
# If fails: check port forwarding is running
ps aux | grep port-forward
# Or set up port forwarding:
kubectl port-forward service/postgres 5433:5432 -n ats-dev &
```

### "Tests failing"
```bash
PYTHONPATH=src pytest tests/integration/specific_test.py -v -s
# Check error messages and debug step by step
```

### "External access not working"
```bash
# Get correct external IP and port (NOT localhost)
kubectl get nodes -o wide
kubectl get service service-name -n ats-dev  
curl -v "http://NODE_IP:NODE_PORT/health"
```

---

## 📚 Next Steps

**After completing this setup, learn more:**

### **🏗️ Explore Platform Components**
- **[🔧 Backend Platform](backend-platform/)** - APIs, services, business logic
- **[📊 Data Infrastructure](data-infrastructure/)** - Data pipelines, storage, ETL
- **[🤖 ML Platform](ml-platform/)** - Training, models, AI optimization
- **[☁️ Online Infrastructure](online-infrastructure/)** - K8s, CI/CD, monitoring

### **📖 Deep Dive Guides**
- **[💻 DEVELOPMENT.md](DEVELOPMENT.md)** - Complete development workflow, testing, CI/CD
- **[🚢 DEPLOYMENT.md](DEPLOYMENT.md)** - Deployment strategies, environments, monitoring

---

## 🎯 Success Criteria

**You're ready to contribute when you can:**
- [ ] Run `python3 scripts/run_dev.py psql --query "SELECT 1"` successfully
- [ ] Deploy a Kubernetes job and see results in database
- [ ] Deploy a service and access it within cluster
- [ ] Write failing test → implement code → see test pass
- [ ] Run integration tests and have them pass  
- [ ] Use kubectl to manage ats-dev namespace resources

---

## 🚨 Critical Rules

### Development Standards (MANDATORY)
- **🎫 JIRA ticket required** before any work
- **🌿 Feature branches only** - NEVER commit to main
- **🔍 Schema validation first** - prevent database errors
- **🧪 TDD required** - tests before code
- **☸️ K8s for everything** - no local scripts
- **🚫 No demo data** - real data only in dev/staging/prod
- **✅ End-to-end validation** - complete pipelines must work

### Deployment Standards  
- **✅ Use GitOps workflows** for all deployments
- **✅ Run safety checks** before deploying
- **✅ Monitor deployments** during rollout
- **✅ Test external access** after deployment
- **✅ Document rollback** procedures for critical changes

### Quality Requirements
- **📊 Schema validation tests** for database changes
- **🔒 Security scanning** for code changes  
- **📈 Performance testing** for critical paths
- **📝 Documentation updates** for new features
- **✅ Code review approval** before merging

---

**🎉 Welcome to ATS! You're now ready to build enterprise algorithmic trading infrastructure.**

*For detailed workflows, see the DEVELOPMENT.md and DEPLOYMENT.md guides.*