# 📖 ATS Platform Documentation Hub

**Enterprise algorithmic trading platform - Complete documentation in 9 essential guides**

---

## 🚀 **Quick Start (New Users)**

**Get running in 15 minutes:**
1. **[01_QUICKSTART_GUIDE.md](01_QUICKSTART_GUIDE.md)** ⭐ **Essential 15-minute setup and core concepts**
2. **[02_DEVELOPMENT_GUIDE.md](02_DEVELOPMENT_GUIDE.md)** ⭐ **Complete development workflow and TDD**
3. **[03_INFRASTRUCTURE_OPERATIONS.md](03_INFRASTRUCTURE_OPERATIONS.md)** ⭐ **Infrastructure and daily operations**

---

## 📚 **Complete Documentation (9 Essential Guides)**

### **🎯 Core Platform**
1. **[01_QUICKSTART_GUIDE.md](01_QUICKSTART_GUIDE.md)** - 15-minute setup, service architecture, verification
2. **[02_DEVELOPMENT_GUIDE.md](02_DEVELOPMENT_GUIDE.md)** - TDD workflow, testing, quality standards
3. **[03_INFRASTRUCTURE_OPERATIONS.md](03_INFRASTRUCTURE_OPERATIONS.md)** - Docker, databases, deployment, monitoring

### **🔧 Platform Integration**
4. **[04_API_CONFIGURATION.md](04_API_CONFIGURATION.md)** - API endpoints, authentication, environment config
5. **[05_DATA_ML_PLATFORM.md](05_DATA_ML_PLATFORM.md)** - Data pipelines, ML training, feature engineering
6. **[06_MONITORING_SERVICES.md](06_MONITORING_SERVICES.md)** - Service management, alerts, automation

### **📈 Advanced Features**
7. **[07_NEWS_EVENTS_SYSTEM.md](07_NEWS_EVENTS_SYSTEM.md)** - News collection, LLM analysis, signal generation
8. **[08_TROUBLESHOOTING_FAQ.md](08_TROUBLESHOOTING_FAQ.md)** - Common issues, emergency procedures, debugging
9. **[09_PROJECT_SPECIFICATIONS.md](09_PROJECT_SPECIFICATIONS.md)** - Active projects, features, roadmap

---

## 🎯 **Role-Based Quick Paths**

### **🔧 Backend Engineers**
→ **[01_QUICKSTART_GUIDE.md](01_QUICKSTART_GUIDE.md)** → **[04_API_CONFIGURATION.md](04_API_CONFIGURATION.md)** → **[02_DEVELOPMENT_GUIDE.md](02_DEVELOPMENT_GUIDE.md)**

### **📊 Data Engineers**
→ **[01_QUICKSTART_GUIDE.md](01_QUICKSTART_GUIDE.md)** → **[05_DATA_ML_PLATFORM.md](05_DATA_ML_PLATFORM.md)** → **[02_DEVELOPMENT_GUIDE.md](02_DEVELOPMENT_GUIDE.md)**

### **🤖 Data Scientists**
→ **[01_QUICKSTART_GUIDE.md](01_QUICKSTART_GUIDE.md)** → **[05_DATA_ML_PLATFORM.md](05_DATA_ML_PLATFORM.md)** → **[07_NEWS_EVENTS_SYSTEM.md](07_NEWS_EVENTS_SYSTEM.md)**

### **☁️ DevOps Engineers**
→ **[03_INFRASTRUCTURE_OPERATIONS.md](03_INFRASTRUCTURE_OPERATIONS.md)** → **[06_MONITORING_SERVICES.md](06_MONITORING_SERVICES.md)** → **[08_TROUBLESHOOTING_FAQ.md](08_TROUBLESHOOTING_FAQ.md)**

### **👥 Product Managers**
→ **[01_QUICKSTART_GUIDE.md](01_QUICKSTART_GUIDE.md)** → **[09_PROJECT_SPECIFICATIONS.md](09_PROJECT_SPECIFICATIONS.md)** → All platform docs

---

## ⚡ **Emergency Quick Reference**

### **🆘 Critical Issues**
```bash
# System health check
python scripts/run_dev.py status && python scripts/run_intg.py status
python scripts/run_dev.py query --query "SELECT 1"

# Emergency contacts
Slack: #incident-response
Platform Team: @team-lead
```

### **📊 Key Commands**
```bash
# Development
python scripts/run_dev.py setup                    # Complete dev environment setup
python scripts/run_dev.py query --query "SQL"     # Database queries
python scripts/run_dev.py test                     # Run test suite
python scripts/run_dev.py start --service postgres # Start services

# Operations
python scripts/run_intg.py status                  # Integration environment status
curl -f http://localhost:3000/health              # ATS-DEV health
curl -f http://localhost:4000/health              # ATS-INTG health
```

### **🌐 Service Endpoints Quick Reference**
```bash
# ATS-DEV Environment (Development)
http://localhost:3000/health          # Analytics health check
http://localhost:3000/eda            # EDA Dashboard interface
http://localhost:8000/health          # API health check
postgresql://postgres:dev_password@localhost:3432/dev_db

# ATS-INTG Environment (Integration Testing)
http://localhost:4000/health          # Analytics health check
http://localhost:4000/eda            # EDA Dashboard interface
http://localhost:4080/metrics        # Prometheus metrics
http://localhost:4002/               # Grafana dashboards (admin/admin)
postgresql://postgres:intg_password@localhost:4432/intg_db

# Service discovery
python scripts/run_dev.py status     # Check running services
docker ps | grep -E "(ats-dev|intg)"  # Container status with ports
```

---

## 🏗️ **What Is ATS?**

**ATS is a Docker-first fintech platform** for algorithmic trading:

### **🎯 Core Capabilities**
- **AI-Powered Portfolio Recommendations** - Hourly ML-driven investment signals
- **📊 Multi-Vendor Data Infrastructure** - Polygon, Tiingo, EODHD, FMP integration
- **🔑 Centralized API Key Management** - Automatic authentication across all vendors
- **🤖 Smart Money Zone Detection** - Institutional flow analysis and pattern recognition
- **🐳 Enterprise Docker Architecture** - Scalable, reliable, production-ready
- **🔄 Complete MLOps Pipeline** - Automated training, validation, deployment, monitoring

### **🚀 Technical Architecture**
- **Languages**: Python, SQL, JavaScript
- **Databases**: PostgreSQL with TimescaleDB
- **Containers**: Docker with GPU support
- **ML Platform**: PyTorch, Ray distributed computing
- **APIs**: RESTful APIs with real-time data
- **Testing**: Pytest, Playwright for browser testing
- **Monitoring**: Prometheus, Grafana, SigNoz

### **🎯 Target Users**
Retail traders, RIAs, family offices, institutional investors

---

## 🚨 **Critical Development Rules**

### **Mandatory Development Standards**
- **🐳 Docker-First** - All development in Docker containers (`python scripts/run_dev.py`)
- **🧪 Test-Driven Development** - Write failing tests first, then implement
- **🚫 No Demo Data** - Real data only in dev/staging/prod environments
- **✅ End-to-End Validation** - Complete pipeline must work before claiming done
- **🔍 Schema Validation First** - Prevent database errors with validation
- **🎫 GitHub Issue Required** - Before any work
- **🌿 Feature Branches Only** - NEVER commit to main

### **Zero Tolerance Policies**
- **No mock/synthetic data** outside unit tests
- **No new files** without exhaustive search for existing functionality
- **No superficial testing** (file exists, 200 OK responses)
- **No completion claims** without thorough validation
- **Debug-first methodology** - understand root causes before fixes

---

## 📋 **Active Projects**

### **🏗️ Multi-Timeframe OHLC Signals System**
**Status**: Production Ready | **Timeline**: Deployed | **Owner**: ML Team

Complete multi-timeframe technical analysis signals for enhanced trading decisions.
- **Quick Links**: [09_PROJECT_SPECIFICATIONS.md#multi-timeframe-ohlc-signals](09_PROJECT_SPECIFICATIONS.md#1-multi-timeframe-ohlc-signals-system)

### **🎨 ATS EDA (Exploratory Data Analysis) Tool**
**Status**: Production Deployed | **Timeline**: Active | **Owner**: Analytics Team

Interactive web-based tool for exploring training datasets, sequences, and feature analysis.
- **Access**: http://localhost:3000/eda
- **Quick Links**: [09_PROJECT_SPECIFICATIONS.md#ats-eda-tool](09_PROJECT_SPECIFICATIONS.md#2-ats-eda-exploratory-data-analysis-tool)

### **📰 LLM News Signal Extraction System**
**Status**: Integration Testing | **Timeline**: Q1 2025 | **Owner**: NLP Team

Extract trading signals from news articles using large language models and sentiment analysis.
- **Quick Links**: [07_NEWS_EVENTS_SYSTEM.md](07_NEWS_EVENTS_SYSTEM.md) | [09_PROJECT_SPECIFICATIONS.md#llm-news-signals](09_PROJECT_SPECIFICATIONS.md#3-llm-news-signal-extraction-system)

---

## ✅ **Documentation Validation**

### **Coverage Verification**
All essential ATS platform functionality is covered across the 9 guides:

- ✅ **Setup & Onboarding** - Covered in Guide 1
- ✅ **Development Workflow** - Covered in Guide 2
- ✅ **Infrastructure & Operations** - Covered in Guide 3
- ✅ **API Integration** - Covered in Guide 4
- ✅ **Data & ML Platform** - Covered in Guide 5
- ✅ **Monitoring & Services** - Covered in Guide 6
- ✅ **News & Events** - Covered in Guide 7
- ✅ **Troubleshooting** - Covered in Guide 8
- ✅ **Project Specifications** - Covered in Guide 9

### **No Duplication**
- Each guide covers distinct functionality areas
- Cross-references used instead of content duplication
- All guides under 30k characters
- Total consolidation: 37 original docs → 9 comprehensive guides

---

## 🎯 **Success Criteria**

**You're ready to contribute when you can:**
- [ ] Complete the 15-minute setup in Guide 1
- [ ] Deploy a service and access it externally
- [ ] Write failing test → implement code → see test pass
- [ ] Navigate to the right documentation for your role
- [ ] Resolve common issues using the troubleshooting guide

---

**🎉 Welcome to ATS! These 9 comprehensive guides contain everything you need to build enterprise algorithmic trading infrastructure.**

**📖 For immediate help: Start with [01_QUICKSTART_GUIDE.md](01_QUICKSTART_GUIDE.md)**