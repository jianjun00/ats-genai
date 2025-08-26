# 🚀 ATS Platform - START HERE

**Get up and running with the ATS algorithmic trading platform in 15 minutes.**

---

## 📋 What Is ATS?

**ATS is a Docker + GPU-enabled fintech platform** for algorithmic trading:
- **🏗️ Enterprise Architecture**: Market-neutral portfolio optimization, multi-vendor data, Smart Money Zones
- **🐳 Docker-First**: Everything runs in Docker containers with GPU support
- **🧪 Test-Driven**: Write failing tests first, then implement
- **🔄 End-to-End**: Real data → Database → API → Frontend → Tests pass

---

## ⚡ 5-Minute Setup

### 1. Clone and Install (2 min)
```bash
git clone https://github.com/AkoloTechnologies/ats-genai
cd ats-genai
uv sync
```

### 2. Setup Dev Environment (2 min)
```bash
# Setup complete development environment
python scripts/run_dev.py setup
# ✅ Success = Docker PostgreSQL started, database ready
# ❌ Fails = check Docker Desktop is running
```

### 3. Run First Test (1 min)
```bash
python scripts/run_dev.py test --test tests/integration/test_analytics_platform_integration.py
# ✅ Pass = environment ready
# ❌ Fail = check troubleshooting below
```

---

## 🎯 Core Concepts (10 minutes)

### 🐳 Docker-First Development

**✅ ALWAYS Use:**
```bash
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_daily_prices"
python scripts/run_dev.py run --script scripts/data_generation/create_sample_data.py
python scripts/run_dev.py start --service postgres  # Start services
python scripts/run_dev.py status                 # Check running services
```

**❌ NEVER Use:**
```bash
docker run ...                          # Use python scripts/run_dev.py instead
docker-compose up                       # Use run_dev service management
```

### 🧪 Test-Driven Development (TDD)

**MANDATORY sequence for ALL code changes:**
```bash
# 1. Write failing test FIRST
touch tests/integration/test_new_feature.py
python scripts/run_dev.py test --test tests/integration/test_new_feature.py
# ✅ Should FAIL (proves test works)

# 2. Write minimal code to make test pass
# (implement your feature in src/)

# 3. Verify test passes
python scripts/run_dev.py test --test tests/integration/test_new_feature.py
# ✅ Should PASS

# 4. Run full test suite
python scripts/run_dev.py test
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
# Run data pipeline
python scripts/run_dev.py deploy --file k8s/data-pipeline-job.yaml
# Verify data quality
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_daily_prices WHERE symbol IN ('AAPL', 'MSFT')"
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
# Generate training data
python scripts/run_dev.py deploy --file k8s/enhanced-training-job.yaml
# Verify dataset
python scripts/run_dev.py query --query "SELECT dataset_name, total_sequences FROM dev_training_dataset ORDER BY id DESC LIMIT 5"
```

---

## ✅ Verification Commands

**Run these to verify your setup works:**
```bash
# 1. Database connectivity
python scripts/run_dev.py query --query "SELECT version()"

# 2. Job execution capability  
python scripts/run_dev.py status

# 3. External service access
curl -s "http://$(kubectl get nodes -o wide | awk 'NR==2{print $6}'):32090/health" | jq

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
# If works: check dev CLI exists at scripts/dev_cli.py
```

### "Database connection failed"  
```bash
python scripts/run_dev.py query --query "SELECT 1"
# If fails: check port forwarding is running
ps aux | grep port-forward
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
- [ ] Run `python scripts/run_dev.py query --query "SELECT 1"` successfully
- [ ] Execute a data job and see results in database
- [ ] Deploy a webapp and access it via external IP
- [ ] Write failing test → implement code → see test pass
- [ ] Run integration tests and have them pass  
- [ ] Access services externally (not just port-forwarding)

---

## 🚨 Critical Rules

### Development Standards (MANDATORY)
- **🎫 GitHub Issue required** before any work
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