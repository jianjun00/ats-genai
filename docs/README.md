# ATS Documentation Hub

Welcome to the ATS fintech platform documentation. This hub provides quick access to all documentation organized by role and purpose.

## 🚀 Quick Start

**New to the team?** Start here:
1. [Quick Start Guide](onboarding/QUICK_START.md) - Get up and running in 15 minutes
2. [Development Setup](onboarding/DEVELOPMENT_SETUP.md) - Complete dev environment
3. [Architecture Overview](onboarding/ARCHITECTURE_OVERVIEW.md) - Understand the system

## 👥 Role-Based Documentation

Find documentation specific to your role:

| Role | Documentation | Key Focus |
|------|---------------|-----------|
| **Product Manager** | [PM Guide](roles/PRODUCT_MANAGER.md) | Product strategy, requirements, metrics |
| **Backend Engineer** | [Backend Guide](roles/BACKEND_ENGINEER.md) | APIs, services, infrastructure |
| **Frontend Engineer** | [Frontend Guide](roles/FRONTEND_ENGINEER.md) | UI/UX, dashboards, visualizations |
| **Data Engineer** | [Data Guide](roles/DATA_ENGINEER.md) | Pipelines, storage, processing |
| **Model Developer** | [ML Guide](roles/MODEL_DEVELOPER.md) | ML models, training, evaluation |
| **Release Engineer** | [DevOps Guide](roles/RELEASE_ENGINEER.md) | CI/CD, deployment, monitoring |
| **Oncall Support** | [Support Guide](roles/ONCALL_SUPPORT.md) | Incident response, troubleshooting |

## 🛠️ Development Documentation

Core development resources:

- **[Development Workflow](development/DEVELOPMENT_WORKFLOW.md)** - TDD process, testing, validation
- **[Kubernetes Guide](development/KUBERNETES_GUIDE.md)** - K8s-first development
- **[Testing Guide](development/TESTING_GUIDE.md)** - Testing strategies and best practices
- **[Debugging Guide](development/DEBUGGING_GUIDE.md)** - Common issues and solutions

## 🏗️ Architecture Documentation

System design and architecture:

- **[System Architecture](architecture/SYSTEM_ARCHITECTURE.md)** - High-level ML system design
- **[Database Design](architecture/DATABASE_DESIGN.md)** - Schema and data modeling
- **[API Design](architecture/API_DESIGN.md)** - REST API specifications  
- **[Infrastructure](INFRASTRUCTURE.md)** - K8s, containers, networking

## 🚀 Operations Documentation

Deployment and operations:

- **[Deployment Guide](operations/DEPLOYMENT.md)** - Production deployments
- **[Credential Management](operations/CREDENTIAL_MANAGEMENT_GUIDE.md)** - Security and authentication
- **[Monitoring](POSTGRESQL_MONITORING_INTEGRATION.md)** - Observability and alerting
- **[Troubleshooting](operations/TROUBLESHOOTING.md)** - Operational issues
- **[Runbooks](operations/RUNBOOKS.md)** - Emergency procedures

## 📋 Common Commands

**Development:**
```bash
# Use dev CLI for all operations
python scripts/dev_cli.py query "SELECT COUNT(*) FROM dev_daily_prices"
python scripts/dev_cli.py job price-unification --symbols AAPL,MSFT

# Testing
PYTHONPATH=src pytest tests/integration/ -v
```

**Infrastructure:**
```bash
# Never use kubectl directly - use dev CLI
python scripts/dev_cli.py list
python scripts/dev_cli.py logs job-name
```

## 🚨 Critical Reminders

- **Kubernetes-First Development**: Always use Kubernetes for dev operations
- **Test-Driven Development**: Write tests before code changes
- **End-to-End Validation**: Verify complete data pipelines work
- **Use Existing Infrastructure**: Don't rebuild what already works

## 🆘 Getting Help

- **Development Issues**: Check [Debugging Guide](development/DEBUGGING_GUIDE.md)
- **Infrastructure Problems**: See [Troubleshooting](operations/TROUBLESHOOTING.md)
- **Emergency Incidents**: Follow [Runbooks](operations/RUNBOOKS.md)
- **Onboarding Questions**: Start with [Quick Start](onboarding/QUICK_START.md)

---

*This documentation is organized for quick navigation. Each role has focused, actionable guidance without information overload.*