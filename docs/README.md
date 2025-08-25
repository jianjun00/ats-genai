# ATS Platform Documentation

**Welcome to the ATS Platform comprehensive documentation hub.**

## 🚀 Quick Start

**New to ATS Platform? Start here:**

- **[15-Minute Setup](onboarding/QUICK_START.md)** - Get running immediately
- **[Development Setup](development/UNIFIED_DEVELOPMENT_WORKFLOW.md)** - Complete environment setup
- **[Architecture Overview](architecture/SYSTEM_ARCHITECTURE.md)** - Understand the system

## 📚 Core Documentation

### 🎯 **Essential Guides** (Start Here)
- **[🚀 START_HERE.md](START_HERE.md)** ⭐ **15-minute setup, core concepts, troubleshooting**
- **[💻 DEVELOPMENT.md](DEVELOPMENT.md)** ⭐ **Complete development workflow, testing, CI/CD**
- **[🚢 DEPLOYMENT.md](DEPLOYMENT.md)** ⭐ **All deployment strategies, monitoring, operations**

---

## 🏗️ Platform Components

### 🔧 **[Backend Platform](backend-platform/)**
**APIs, Services, Business Logic**
- [Component Overview](backend-platform/README.md) - Service architecture and dependencies
- [System Design](backend-platform/SYSTEM_DESIGN.md) - API design, database schema, security
- [Operations](backend-platform/OPERATIONS.md) - Deployment, monitoring, troubleshooting
- [PRDs & DRDs](backend-platform/prd/) - Product and detailed requirements

### 📊 **[Data Infrastructure](data-infrastructure/)**  
**Data Pipelines, Storage, ETL**
- [Component Overview](data-infrastructure/README.md) - Multi-vendor data management
- [System Design](data-infrastructure/SYSTEM_DESIGN.md) - Data flows, quality, reconciliation
- [Operations](data-infrastructure/OPERATIONS.md) - Pipeline monitoring, data ops
- [PRDs & DRDs](data-infrastructure/prd/) - Data platform requirements

### 🤖 **[ML Platform](ml-platform/)**
**Training, Models, AI-Powered Optimization**
- [Component Overview](ml-platform/README.md) - ML lifecycle and model inventory
- [System Design](ml-platform/SYSTEM_DESIGN.md) - Training pipelines, inference architecture
- [Operations](ml-platform/OPERATIONS.md) - MLOps, model monitoring, performance
- [PRDs & DRDs](ml-platform/prd/) - ML platform and model requirements

### ☁️ **[Online Infrastructure](online-infrastructure/)**
**Kubernetes, CI/CD, Monitoring**
- [Component Overview](online-infrastructure/README.md) - Platform orchestration and operations
- [System Design](online-infrastructure/SYSTEM_DESIGN.md) - K8s architecture, security, networking
- [Operations](online-infrastructure/OPERATIONS.md) - DevOps, monitoring, incident response
- [PRDs & DRDs](online-infrastructure/prd/) - Infrastructure platform requirements

---

## 👥 Role-Based Learning Paths

### 🔧 **Backend Engineers**
1. [🚀 START_HERE.md](START_HERE.md) - Platform overview and setup
2. [🔧 Backend Platform](backend-platform/) - Service architecture and APIs
3. [💻 DEVELOPMENT.md](DEVELOPMENT.md) - Development workflow and testing

### 📊 **Data Engineers**  
1. [🚀 START_HERE.md](START_HERE.md) - Platform overview and setup
2. [📊 Data Infrastructure](data-infrastructure/) - Data pipelines and operations
3. [💻 DEVELOPMENT.md](DEVELOPMENT.md) - Development workflow and testing

### 🤖 **Data Scientists**
1. [🚀 START_HERE.md](START_HERE.md) - Platform overview and setup  
2. [🤖 ML Platform](ml-platform/) - Model development and deployment
3. [📊 Data Infrastructure](data-infrastructure/) - Available datasets and access

### ☁️ **DevOps Engineers**
1. [🚀 START_HERE.md](START_HERE.md) - Platform overview and setup
2. [☁️ Online Infrastructure](online-infrastructure/) - Platform operations
3. [🚢 DEPLOYMENT.md](DEPLOYMENT.md) - Deployment strategies and monitoring

### 📋 **Product Managers**
1. [🚀 START_HERE.md](START_HERE.md) - Platform overview  
2. Component PRDs: [Backend](backend-platform/prd/), [Data](data-infrastructure/prd/), [ML](ml-platform/prd/), [Infrastructure](online-infrastructure/prd/)
3. [Product Requirements](product/) - Cross-component planning

## 🔧 Operational Scripts

The platform includes comprehensive automation scripts:

### Development Workflow Scripts
```bash
# Pre-deployment safety checks
./scripts/pre_deploy_check.sh

# Deploy with team coordination
./scripts/dev_deploy.sh

# Monitor deployment progress
./scripts/monitor_deployment.sh <service-name>
```

### Deployment Management
```bash
# Comprehensive status overview
./scripts/deployment_status.sh

# External access information
./scripts/get_external_access.sh all

# Multiple rollback strategies
./scripts/rollback_deployment.sh <service-name> [k8s|git|argocd]
```

### ArgoCD Integration
```bash
# Force ArgoCD synchronization
./scripts/force_argocd_sync.sh

# Validate deployment files
./scripts/validate_deployment.sh k8s/**/*.yaml

# Detect resource conflicts
python scripts/detect_k8s_conflicts.py k8s/
```

## 📋 Documentation Organization

### Current Structure
```
docs/
├── README.md                    # This file - documentation hub
├── development/                 # Development processes and tools
│   ├── UNIFIED_DEVELOPMENT_WORKFLOW.md  # Complete development workflow
│   ├── UNIFIED_CICD_GUIDE.md           # CI/CD and GitOps processes
│   ├── GITOPS_DEVELOPMENT_WORKFLOW.md  # Option 2 GitOps workflow
│   ├── KUBERNETES_GUIDE.md             # K8s development patterns
│   └── TESTING_GUIDE.md               # Testing strategies
├── operations/                  # Deployment and operations
│   ├── UNIFIED_DEPLOYMENT_GUIDE.md    # All deployment strategies
│   ├── ARGOCD_TROUBLESHOOTING.md      # ArgoCD issues and solutions
│   ├── MONITORING.md                  # System monitoring
│   └── CREDENTIAL_MANAGEMENT_GUIDE.md # Security management
├── architecture/                # System design and architecture
│   ├── SYSTEM_ARCHITECTURE.md         # High-level system design
│   ├── DATABASE_DESIGN.md             # Database schema and modeling
│   └── INFRASTRUCTURE.md              # Infrastructure patterns
├── roles/                      # Role-specific documentation
│   ├── PRODUCT_MANAGER.md             # PM-focused documentation
│   └── BACKEND_ENGINEER.md            # Engineering-focused docs
├── onboarding/                 # New user onboarding
│   ├── QUICK_START.md                 # 15-minute setup guide
│   ├── DEVELOPMENT_SETUP.md           # Complete setup process
│   └── ARCHITECTURE_OVERVIEW.md       # System overview
└── archive/                    # Legacy and archived documentation
    └── pre-consolidation/             # Archived duplicate docs
```

## 🚨 Critical Development Rules

**Every developer must follow these mandatory rules:**

### 1. **Unified Development Workflow** (MANDATORY)
- 🎫 **Create JIRA ticket** before any work
- 🌿 **Use feature branches** - NEVER commit to main
- 🗄️ **Validate database schema** before coding
- 🧪 **Write failing tests first** (TDD)
- 🚢 **Use Kubernetes** for all development operations
- 🚫 **No demo data** in dev/staging/production
- 🔍 **Integration tests required** for all features

### 2. **Deployment Standards**
- ✅ **Use GitOps Option 2** workflow for deployments
- ✅ **Run safety checks** before deploying
- ✅ **Monitor deployments** during rollout
- ✅ **Test external access** after deployment
- ✅ **Document rollback procedures** for critical changes

### 3. **Quality Requirements**
- 📊 **Schema validation tests** for all database changes
- 🔒 **Security scanning** for all code changes
- 📈 **Performance testing** for critical paths
- 📝 **Documentation updates** for new features
- ✅ **Code review approval** before merging

## 🔄 Workflow Quick Reference

### Starting New Work
```bash
# 1. Create JIRA ticket first
# 2. Create feature branch
git checkout -b PGPT-1234/feature-description

# 3. Run safety checks
./scripts/pre_deploy_check.sh

# 4. Follow TDD process (write test first)
# 5. Implement feature
# 6. Deploy and verify
./scripts/dev_deploy.sh
./scripts/monitor_deployment.sh your-service
```

### Emergency Procedures
```bash
# Quick rollback
./scripts/rollback_deployment.sh your-service k8s

# Check system status
./scripts/deployment_status.sh

# Force ArgoCD sync
./scripts/force_argocd_sync.sh --force
```

## 📖 Learning Path

### For New Developers
1. **Read**: [15-Minute Setup](onboarding/QUICK_START.md)
2. **Follow**: [Unified Development Workflow](development/UNIFIED_DEVELOPMENT_WORKFLOW.md)
3. **Practice**: Deploy a simple change using the workflow
4. **Study**: [System Architecture](architecture/SYSTEM_ARCHITECTURE.md)

### For DevOps Engineers
1. **Read**: [Unified Deployment Guide](operations/UNIFIED_DEPLOYMENT_GUIDE.md)
2. **Study**: [GitOps Development Workflow](development/GITOPS_DEVELOPMENT_WORKFLOW.md)
3. **Master**: [ArgoCD Troubleshooting](operations/ARGOCD_TROUBLESHOOTING.md)
4. **Implement**: [CI/CD Guide](development/UNIFIED_CICD_GUIDE.md)

### For Product Managers
1. **Start**: [Product Manager Guide](roles/PRODUCT_MANAGER.md)
2. **Understand**: [System Architecture](architecture/SYSTEM_ARCHITECTURE.md)
3. **Learn**: [Monitoring Guide](operations/MONITORING.md)

## ❓ Getting Help

### Documentation Issues
- **Missing information?** Check the [archive](archive/) for legacy docs
- **Contradictory guidance?** Follow the **UNIFIED** guides (marked with ⭐)
- **Broken links?** File an issue or update the documentation

### Technical Support
- **Development issues**: [Unified Development Workflow](development/UNIFIED_DEVELOPMENT_WORKFLOW.md)
- **Deployment problems**: [ArgoCD Troubleshooting](operations/ARGOCD_TROUBLESHOOTING.md)
- **System issues**: [Monitoring Guide](operations/MONITORING.md)

### Emergency Contacts
- **Critical production issues**: Follow [incident response procedures](operations/MONITORING.md#incident-response)
- **Security incidents**: Follow [security procedures](operations/CREDENTIAL_MANAGEMENT_GUIDE.md#incident-response)

---

## 📊 Documentation Metrics

**Post-consolidation statistics:**
- **75% reduction** in documentation files (165 → ~40)
- **90% elimination** of duplicate content
- **Single source of truth** for each process
- **Clear navigation paths** for all user types

---

**🎯 This documentation hub provides everything needed for successful ATS platform development, deployment, and operations.**