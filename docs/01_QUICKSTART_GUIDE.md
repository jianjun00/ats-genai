# 🚀 ATS Platform - Quickstart Guide

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

## 🌐 Service Architecture

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

### Service Discovery Commands
```bash
# Check running services and their ports
python scripts/run_dev.py status                    # ATS-DEV services
docker ps | grep -E "(ats-dev|intg)"               # Container status with ports

# Test endpoints are working
curl -f http://localhost:3000/health               # ATS-DEV analytics
curl -f http://localhost:4000/health               # ATS-INTG analytics  
curl -f http://localhost:4080/health               # ATS-INTG metrics
```

---

## 🎯 Core Development Interface

### Primary Commands (Use These Only)
```bash
# Development environment setup
python scripts/run_dev.py setup                    # Setup environment
python scripts/run_dev.py query --query "SQL"     # Database queries  
python scripts/run_dev.py test                     # Run tests
python scripts/run_dev.py start --service postgres # Start services
python scripts/run_dev.py status                   # Check services

# Integration environment
python scripts/run_intg.py start --service analytics
python scripts/run_intg.py status
```

### Forbidden Commands (Never Use)
```bash
docker run ...                          # Use python scripts/run_dev.py instead
docker-compose up                       # Use run_dev service management
```

---

## 👤 Role-Based Quick Actions

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
curl -s "http://localhost:3000/health" | jq

# 4. Integration tests  
PYTHONPATH=src pytest tests/integration/ -v --tb=short
```

---

## 🆘 Common Issues & Quick Fixes

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
curl -v "http://localhost:3000/health"
```

---

## 🎯 Success Criteria

**You're ready for development when you can:**
- [ ] Run `python scripts/run_dev.py query --query "SELECT 1"` successfully
- [ ] Execute a data job and see results in database
- [ ] Start a service and access it via localhost
- [ ] Access services via Docker networking
- [ ] Navigate to next steps in development guides

---

## 📚 Next Steps

**After completing this setup:**

1. **[02_DEVELOPMENT_GUIDE.md](02_DEVELOPMENT_GUIDE.md)** - Complete development workflow, TDD, testing
2. **[03_INFRASTRUCTURE_OPERATIONS.md](03_INFRASTRUCTURE_OPERATIONS.md)** - Infrastructure setup and operations
3. **[04_API_CONFIGURATION.md](04_API_CONFIGURATION.md)** - API references and configuration
4. **[05_DATA_ML_PLATFORM.md](05_DATA_ML_PLATFORM.md)** - Data pipelines and ML training
5. **[09_PROJECT_SPECIFICATIONS.md](09_PROJECT_SPECIFICATIONS.md)** - Active projects and features

---

## 🚨 Critical Development Rules

### Development Standards (MANDATORY)
- **🎫 GitHub Issue required** before any work  
- **🌿 Feature branches only** - NEVER commit to main
- **🔍 Schema validation first** - prevent database errors
- **🧪 TDD required** - tests before code
- **🐳 Docker for everything** - use run_dev.py and run_intg.py
- **🚫 No demo data** - real data only in dev/staging/prod
- **✅ End-to-end validation** - complete pipelines must work

---

**🎉 Welcome to ATS! You're now ready to build enterprise algorithmic trading infrastructure.**