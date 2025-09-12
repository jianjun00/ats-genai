# 🚀 ATS Platform - Quick Setup

**Get the ATS algorithmic trading platform running in 15 minutes.**

---

## 📋 What Is ATS?

**Docker + GPU-enabled fintech platform** for algorithmic trading:
- **Enterprise Architecture**: Market-neutral portfolio optimization, multi-vendor data
- **Docker-First**: Everything runs in containers with GPU support
- **Test-Driven**: Write failing tests first, then implement
- **End-to-End**: Real data → Database → API → Frontend

---

## ⚡ 5-Minute Setup

### 1. Clone and Install
```bash
git clone https://github.com/AkoloTechnologies/ats-genai
cd ats-genai
uv sync
```

### 2. Setup Environment
```bash
python scripts/run_dev.py setup
# ✅ Success = Docker PostgreSQL started, database ready
```

### 3. Run First Test
```bash
python scripts/run_dev.py test --test tests/integration/test_analytics_platform_integration.py
# ✅ Pass = environment ready
```

---

## 🎯 Core Concepts

### Docker-First Development
**✅ ALWAYS Use:**
```bash
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_daily_prices"
python scripts/run_dev.py run --script scripts/data_generation/create_sample_data.py
python scripts/run_dev.py start --service postgres
python scripts/run_dev.py status
```

**❌ NEVER Use:**
```bash
docker run ...                  # Use run_dev.py instead
docker-compose up               # Use run_dev service management
```

### Test-Driven Development (TDD)
**MANDATORY sequence for ALL code changes:**
```bash
# 1. Write failing test FIRST
touch tests/integration/test_new_feature.py
python scripts/run_dev.py test --test tests/integration/test_new_feature.py
# ✅ Should FAIL

# 2. Write code to make test pass
# 3. Verify test passes
python scripts/run_dev.py test --test tests/integration/test_new_feature.py
# ✅ Should PASS

# 4. Run full test suite
python scripts/run_dev.py test
```

### End-to-End Validation
**Features complete when entire pipeline works:**
1. Generate real data using Docker services
2. Store in database with correct schema  
3. API serves data to external clients
4. Frontend displays data in browser
5. All integration tests pass

---

## 🌐 API Endpoints

### ATS-DEV Environment
```bash
# Analytics Service
http://localhost:3000/health
http://localhost:3000/eda
http://localhost:3000/api/

# Database
postgresql://postgres:dev_password@localhost:3432/dev_db
```

### ATS-INTG Environment
```bash
# Analytics Service
http://localhost:4000/health
http://localhost:4000/eda

# Monitoring
http://localhost:4080/metrics      # Prometheus
http://localhost:4002/             # Grafana (admin/admin)

# Database
postgresql://postgres:intg_password@localhost:4432/intg_db
```

---

## 👤 Quick Actions

### Backend Engineer
```bash
python scripts/run_dev.py start --service api
curl -s "http://localhost:8000/health" | jq
```

### Data Engineer
```bash
# Populate instrument universe
python scripts/run_dev.py run --script scripts/run_tiingo_bulk.py
python scripts/run_dev.py run --script scripts/run_eodhd_bulk.py

# Verify population
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_instrument_tiingo"
```

### Frontend Engineer  
```bash
python scripts/run_dev.py start --service analytics
curl -s "http://localhost:3000/health"
```

### Model Developer
```bash
python scripts/run_dev.py query --query "SELECT dataset_name, total_sequences FROM dev_training_dataset ORDER BY id DESC LIMIT 5"
```

---

## ✅ Verification

**Run these to verify setup:**
```bash
# Database connectivity
python scripts/run_dev.py query --query "SELECT version()"

# Service access
curl -s "http://localhost:3000/health" | jq

# Integration tests
PYTHONPATH=src pytest tests/integration/ -v --tb=short
```

---

## 🆘 Common Issues

### "dev CLI not working"
```bash
python scripts/run_dev.py status
# If fails: check Docker is running
```

### "Database connection failed"  
```bash
python scripts/run_dev.py query --query "SELECT 1"
# If fails: check PostgreSQL service
```

### "Tests failing"
```bash
PYTHONPATH=src pytest tests/integration/specific_test.py -v -s
```

---

## 🎯 Success Criteria

**Ready to contribute when you can:**
- [ ] Run `python scripts/run_dev.py query --query "SELECT 1"`
- [ ] Execute data job and see database results
- [ ] Start service and access via localhost
- [ ] Write failing test → implement → see test pass
- [ ] Run integration tests successfully

---

## 🚨 Critical Rules

- **🎫 GitHub Issue required** before any work
- **🌿 Feature branches only** - NEVER commit to main
- **🧪 TDD required** - tests before code
- **🐳 Docker for everything** - use run_dev.py
- **🚫 No demo data** - real data only
- **✅ End-to-end validation** - complete pipelines must work

---

**🎉 Welcome to ATS! You're now ready to build enterprise algorithmic trading infrastructure.**

*For detailed workflows, see DEVELOPMENT.md and OPERATIONS.md guides.*