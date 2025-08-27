# 📖 ATS Platform Documentation

**Enterprise algorithmic trading platform - Complete documentation in 10 essential guides**

---

## 🚀 **Quick Start (New Users)**

**Get running in 15 minutes:**
1. **[START_HERE.md](START_HERE.md)** ⭐ **Essential setup and core concepts**
2. **[DEVELOPMENT.md](DEVELOPMENT.md)** ⭐ **Complete development workflow** 
3. **[DEPLOYMENT.md](DEPLOYMENT.md)** ⭐ **All deployment strategies**

---

## 📚 **Complete Documentation (11 Essential Guides)**

### **🎯 Platform Essentials**
1. **[README.md](README.md)** - This navigation hub
2. **[START_HERE.md](START_HERE.md)** - 15-minute setup guide
3. **[DEVELOPMENT.md](DEVELOPMENT.md)** - Development workflow, TDD, CI/CD
4. **[DEPLOYMENT.md](DEPLOYMENT.md)** - Deployment strategies and GitOps

### **🏗️ Technical Architecture**  
5. **[SYSTEM_ARCHITECTURE.md](SYSTEM_ARCHITECTURE.md)** - Complete technical architecture
6. **[BACKEND_PLATFORM.md](BACKEND_PLATFORM.md)** - APIs, services, authentication
7. **[DATA_INFRASTRUCTURE.md](DATA_INFRASTRUCTURE.md)** - Data pipelines, storage, ETL
8. **[DATABASE_ENVIRONMENTS.md](DATABASE_ENVIRONMENTS.md)** - Database setup, ats-dev/ats-intg environments
9. **[ML_PLATFORM.md](ML_PLATFORM.md)** - AI/ML training, inference, optimization

### **⚙️ Operations**
10. **[OPERATIONS.md](OPERATIONS.md)** - DevOps, monitoring, infrastructure management
11. **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** - Common issues, solutions, emergency procedures

### **📋 Project Documentation**
12. **[projects/](projects/)** - Active project requirements and design documents

---

## 🎯 **Role-Based Quick Paths**

### **🔧 Backend Engineers**
→ **[START_HERE.md](START_HERE.md)** → **[BACKEND_PLATFORM.md](BACKEND_PLATFORM.md)** → **[DEVELOPMENT.md](DEVELOPMENT.md)**

### **📊 Data Engineers**
→ **[START_HERE.md](START_HERE.md)** → **[DATA_INFRASTRUCTURE.md](DATA_INFRASTRUCTURE.md)** → **[DEVELOPMENT.md](DEVELOPMENT.md)**

### **🤖 Data Scientists**
→ **[START_HERE.md](START_HERE.md)** → **[ML_PLATFORM.md](ML_PLATFORM.md)** → **[DATA_INFRASTRUCTURE.md](DATA_INFRASTRUCTURE.md)**

### **☁️ DevOps Engineers**
→ **[DEPLOYMENT.md](DEPLOYMENT.md)** → **[OPERATIONS.md](OPERATIONS.md)** → **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)**

### **👥 Product Managers**
→ **[START_HERE.md](START_HERE.md)** → **[SYSTEM_ARCHITECTURE.md](SYSTEM_ARCHITECTURE.md)** → All platform docs

---

## ⚡ **Emergency Quick Reference**

### **🆘 Critical Issues**
```bash
# System health check
kubectl cluster-info && kubectl get pods -n ats-dev
python scripts/run_dev.py query --query "SELECT 1"

# Emergency contacts
Slack: #incident-response
Platform Team: @team-lead
```

### **📊 Key Commands**  
```bash
# Development
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_daily_prices"
python scripts/run_dev.py deploy --file k8s/job.yaml
python scripts/run_dev.py logs --job job-name

# Operations
kubectl get all -n ats-dev
kubectl logs -f deployment/service-name -n ats-dev
```

---

## 🎯 **Success Criteria**

**You're ready to contribute when you can:**
- [ ] Complete the 15-minute setup in START_HERE.md
- [ ] Deploy a service and access it externally  
- [ ] Write failing test → implement code → see test pass
- [ ] Navigate to the right documentation for your role
- [ ] Resolve common issues using TROUBLESHOOTING.md

---

## 📋 **Active Projects**

### **🏗️ 30-Year Daily Price History System**
**Status**: Ready for implementation | **Timeline**: 6-8 weeks | **Owner**: Data Infrastructure Team

Complete historical daily price database covering all US stocks and critical market factor ETFs from 1995-2025.

**Quick Links:**
- **[Project Overview](projects/30year-price-history/README.md)** - Executive summary and getting started
- **[PRD](projects/30year-price-history/PRD_30_Year_Daily_Price_History.md)** - Business requirements and success metrics  
- **[DRD](projects/30year-price-history/DRD_30_Year_Daily_Price_History.md)** - Technical architecture and implementation

**Key Deliverables:**
- ✅ **30-year coverage**: 1995-2025 daily OHLCV data
- ✅ **Complete universe**: 4,000+ stocks, 250+ critical ETFs (including TLT, HYG, UUP)
- ✅ **99.95% accuracy**: Multi-vendor validation (Polygon, EODHD, Alpha Vantage)
- ✅ **<100ms queries**: Optimized for backtesting workloads

---

## 🏗️ **What Is ATS?**

**ATS is a Kubernetes-first fintech platform** for algorithmic trading:
- **🎯 AI-Powered Portfolio Recommendations** - Hourly ML-driven investment signals
- **📊 Multi-Vendor Data Infrastructure** - Polygon, Tiingo, Alpha Vantage, FMP integration  
- **🤖 Smart Money Zone Detection** - Institutional flow analysis and pattern recognition
- **☸️ Enterprise Kubernetes Architecture** - Scalable, reliable, production-ready
- **🔄 Complete MLOps Pipeline** - Automated training, validation, deployment, monitoring

**Target Users:** Retail traders, RIAs, family offices, institutional investors

---

## 🚨 **Critical Development Rules**

- **☸️ Kubernetes-First** - All development in K8s clusters (`python scripts/run_dev.py`)
- **🧪 Test-Driven Development** - Write failing tests first, then implement
- **🚫 No Demo Data** - Real data only in dev/staging/prod environments  
- **✅ End-to-End Validation** - Complete pipeline must work before claiming done
- **🔍 Schema Validation First** - Prevent database errors with validation

---

**🎉 Welcome to ATS! These 11 guides contain everything you need to build enterprise algorithmic trading infrastructure.**