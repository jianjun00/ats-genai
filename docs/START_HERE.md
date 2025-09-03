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
1. **Generate real data** using Docker services
2. **Store in database** with correct schema  
3. **API serves data** to external clients
4. **Frontend displays data** in browser
5. **All integration tests pass**

---

## 🌐 API Endpoints Reference

### ATS-DEV Environment (Development)
```bash
# Analytics Service
http://localhost:3000/health          # Health check endpoint
http://localhost:3000/eda            # EDA Dashboard interface
http://localhost:3000/api/           # Analytics API endpoints

# API Service  
http://localhost:8000/health          # API health check
http://localhost:8000/api/           # Main API endpoints

# Database
postgresql://postgres:dev_password@localhost:3432/dev_db
```

### ATS-INTG Environment (Integration Testing)
```bash
# Analytics Service
http://localhost:4000/health          # Health check endpoint
http://localhost:4000/eda            # EDA Dashboard interface
http://localhost:4000/api/           # Analytics API endpoints

# Monitoring & Metrics
http://localhost:4080/health          # Prometheus metrics health
http://localhost:4080/metrics        # Prometheus metrics endpoint
http://localhost:4002/               # Grafana dashboards (admin/admin)
http://localhost:4091/-/ready        # Prometheus server ready check

# Database
postgresql://postgres:intg_password@localhost:4432/intg_db
```

### How to Find Service Endpoints
```bash
# Check running services and their ports
python scripts/run_dev.py status                    # ATS-DEV services
docker ps | grep -E "(ats-dev|intg)"               # Container status with ports

# Get external access info for any service
./scripts/get_external_access.sh service-name      # Service endpoint discovery script

# Test endpoints are working
curl -f http://localhost:3000/health               # ATS-DEV analytics
curl -f http://localhost:4000/health               # ATS-INTG analytics
curl -f http://localhost:4080/health               # ATS-INTG metrics
```

### Common API Usage Patterns
```bash
# Health checks (always test these first)
curl -s http://localhost:3000/health | jq
curl -s http://localhost:4000/health | jq

# Analytics API example
curl -s http://localhost:3000/api/datasets | jq

# Metrics collection
curl -s http://localhost:4080/metrics | grep "ats_"

# Dashboard access (open in browser)
open http://localhost:3000/eda                     # EDA interface
open http://localhost:4002/                        # Grafana dashboards
```

---

## 👤 Role-Specific Quick Actions

### 🔧 Backend Engineer
```bash
# Start API service
python scripts/run_dev.py start --service api
# Test external access
curl -s "http://localhost:8000/health" | jq
```

### 📊 Data Engineer
```bash
# Populate comprehensive instrument universe (60K+ Tiingo + 50K+ EODHD stocks)
python scripts/run_dev.py run --script scripts/run_tiingo_bulk.py    # All Tiingo stocks including delisted
python scripts/run_dev.py run --script scripts/run_eodhd_bulk.py     # All EODHD US exchange stocks

# Verify instrument population
python scripts/run_dev.py query --query "SELECT COUNT(*) as tiingo_instruments FROM dev_instrument_tiingo"
python scripts/run_dev.py query --query "SELECT COUNT(*) as eodhd_instruments FROM dev_instrument_eodhd"

# Check delisted stocks
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_instrument_tiingo WHERE end_date < '2020-01-01'"
```

### 🎨 Frontend Engineer  
```bash
# Start analytics service
python scripts/run_dev.py start --service analytics
# Test in browser
curl -s "http://localhost:3001/health" | grep -E "(OK|healthy)"
```

### 🤖 Model Developer
```bash
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
curl -s "http://localhost:3001/health" | jq

# 4. Integration tests
PYTHONPATH=src pytest tests/integration/ -v --tb=short
```

---

## 🆘 Common Issues & Fixes

### "dev CLI not working"
```bash
# Check Docker services
python scripts/run_dev.py status
# If fails: check Docker is running
# If works: check dev CLI exists at scripts/run_dev.py
```

### "Database connection failed"  
```bash
python scripts/run_dev.py query --query "SELECT 1"
# If fails: check PostgreSQL service is running
python scripts/run_dev.py status
```

### "Tests failing"
```bash
PYTHONPATH=src pytest tests/integration/specific_test.py -v -s
# Check error messages and debug step by step
```

### "External access not working"
```bash
# Check service is running and get port
python scripts/run_dev.py status
# Test local access first
curl -v "http://localhost:3001/health"
```

---

## 📚 Next Steps

**After completing this setup, learn more:**

### **🏗️ Platform Environments**
- **[🔧 ATS-DEV Environment](DEVELOPMENT.md)** - Development workflow, testing, Docker setup
- **[🚀 ATS-INTG Environment](ATS_INTEGRATION_ENVIRONMENT.md)** - Integration testing, CI/CD pipeline, TimescaleDB
- **[📊 Data Infrastructure](data-infrastructure/)** - Data pipelines, storage, ETL
- **[🤖 ML Platform](ml-platform/)** - Training, models, AI optimization

### **📖 Deep Dive Guides**
- **[💻 DEVELOPMENT.md](DEVELOPMENT.md)** - Complete development workflow, testing, CI/CD
- **[🚢 DEPLOYMENT.md](DEPLOYMENT.md)** - Deployment strategies, environments, monitoring
- **[🗄️ ATS_INTEGRATION_ENVIRONMENT.md](ATS_INTEGRATION_ENVIRONMENT.md)** - Integration environment setup and CI/CD

---

## 🎯 Success Criteria

**You're ready to contribute when you can:**
- [ ] Run `python scripts/run_dev.py query --query "SELECT 1"` successfully
- [ ] Execute a data job and see results in database
- [ ] Start a service and access it via localhost
- [ ] Write failing test → implement code → see test pass
- [ ] Run integration tests and have them pass  
- [ ] Access services via Docker networking

---

## 🚨 Critical Rules

### Development Standards (MANDATORY)
- **🎫 GitHub Issue required** before any work
- **🌿 Feature branches only** - NEVER commit to main
- **🔍 Schema validation first** - prevent database errors
- **🧪 TDD required** - tests before code
- **🐳 Docker for everything** - use run_dev.py and run_intg.py
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