# Quick Start Guide (15 Minutes)

Get up and running with the ATS fintech platform in 15 minutes.

## 🚀 What You Need to Know First

**ATS is a Kubernetes-first fintech platform** for algorithmic trading:
- **Everything runs in Kubernetes** (ats-dev namespace)
- **Use dev CLI** for all operations (never kubectl directly)
- **Test-driven development** is mandatory
- **End-to-end validation** required for all features

## ⚡ 5-Minute Setup

### 1. Clone and Install Dependencies (2 min)

```bash
# Clone repository
git clone https://github.com/your-org/ats-genai
cd ats-genai

# Install dependencies
uv sync
```

### 2. Verify Dev CLI Access (2 min)

```bash
# Test dev CLI - this is your primary interface
python scripts/dev_cli.py query "SELECT 1"

# If successful, you're connected to K8s cluster
# If fails, ask team for cluster access setup
```

### 3. Run Your First Test (1 min)

```bash
# Test the system works
PYTHONPATH=src pytest tests/integration/test_analytics_platform_integration.py::TestAnalyticsPlatformIntegration::test_backend_api_can_start -v

# If passes: ✅ Environment is ready
# If fails: Check [Debugging Guide](../development/DEBUGGING_GUIDE.md)
```

## 🎯 10-Minute Core Concepts

### Kubernetes-First Development

**✅ ALWAYS do this:**
```bash
# Use dev CLI for all operations
python scripts/dev_cli.py query "SELECT COUNT(*) FROM dev_daily_prices"
python scripts/dev_cli.py job price-unification --symbols AAPL,MSFT
python scripts/dev_cli.py logs job-name
```

**❌ NEVER do this:**
```bash
# Don't use kubectl directly
kubectl get pods -n ats-dev

# Don't set environment variables manually  
PYTHONPATH=src ENVIRONMENT=dev DB_HOST=localhost python script.py
```

### Test-Driven Development (TDD)

**Every code change follows this sequence:**

```bash
# 1. Write failing test FIRST
touch tests/integration/test_new_feature.py
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v
# ✅ Should FAIL (proves test works)

# 2. Write minimal code to make test pass
# (implement your feature)

# 3. Verify test passes
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v  
# ✅ Should PASS

# 4. Run full test suite
PYTHONPATH=src pytest tests/ -v
# ✅ All tests pass
```

### End-to-End Validation

**Features aren't complete until entire pipeline works:**

1. **Generate real data** in K8s
2. **Store in database** with correct schema
3. **API serves data** to external clients
4. **Frontend displays data** in browser
5. **All integration tests pass**

## 📋 Role-Specific Quick Actions

### Backend Engineer
```bash
# Deploy new API endpoint
python scripts/dev_cli.py deploy api-endpoint --config new-endpoint.yaml

# Test endpoint works externally
curl -s "http://external-ip:port/api/new-endpoint" | jq
```

### Data Engineer  
```bash
# Run data pipeline
python scripts/dev_cli.py job data-pipeline --symbols AAPL,MSFT --date 2024-01-15

# Verify data quality
python scripts/dev_cli.py query "SELECT COUNT(*) FROM dev_daily_prices WHERE symbol IN ('AAPL', 'MSFT')"
```

### Frontend Engineer
```bash
# Deploy webapp with real data
kubectl apply -f k8s/webapp-deployment.yaml

# Test in browser
curl -s "http://external-ip:port/" | grep "Welcome to ATS"
```

### Model Developer
```bash
# Generate training data
python scripts/dev_cli.py job enhanced-training --symbol TSLA --days-back 120

# Verify training dataset
python scripts/dev_cli.py query "SELECT dataset_name, total_sequences FROM dev_training_datasets ORDER BY id DESC LIMIT 5"
```

## 🔍 Verification Commands

**Run these to verify your setup:**

```bash
# 1. Database connectivity
python scripts/dev_cli.py query "SELECT version()"

# 2. Job execution
python scripts/dev_cli.py list

# 3. External service access
curl -s "http://$(kubectl get nodes -o wide | awk 'NR==2{print $6}'):32090/health" | jq

# 4. Integration tests
PYTHONPATH=src pytest tests/integration/ -v --tb=short
```

## 🆘 Common Issues & Quick Fixes

### "dev CLI not working"
```bash
# Check cluster access
kubectl get pods -n ats-dev

# If fails, you need cluster access - ask team
# If works, check dev CLI script exists
ls -la scripts/dev_cli.py
```

### "Database connection failed"
```bash
# Check database service
python scripts/dev_cli.py query "SELECT 1"

# Check port forwarding is running
ps aux | grep port-forward
```

### "Tests failing"
```bash
# Run specific test with verbose output
PYTHONPATH=src pytest tests/integration/specific_test.py -v -s

# Check [Debugging Guide](../development/DEBUGGING_GUIDE.md) for details
```

### "External access not working"
```bash
# Get correct external IP and port
kubectl get nodes -o wide
kubectl get service service-name -n ats-dev

# Test actual external URL (not localhost)
curl -v "http://NODE_IP:NODE_PORT/health"
```

## 📚 Next Steps (After Quick Start)

**Choose your learning path:**

1. **Complete Development Setup**: [Development Setup Guide](DEVELOPMENT_SETUP.md)
2. **Understand Architecture**: [Architecture Overview](ARCHITECTURE_OVERVIEW.md)  
3. **Learn Your Role**: [Role-Specific Guides](../roles/)
4. **Master Development Workflow**: [Development Workflow](../development/DEVELOPMENT_WORKFLOW.md)
5. **Deep Dive into K8s**: [Kubernetes Guide](../development/KUBERNETES_GUIDE.md)

## 🎯 Success Criteria

**You're ready to contribute when you can:**

- [ ] Run `python scripts/dev_cli.py query "SELECT 1"` successfully
- [ ] Execute a data processing job and see results
- [ ] Deploy a webapp and access it externally  
- [ ] Write a failing test, fix code, see test pass
- [ ] Run integration tests and have them pass
- [ ] Access services via external IP (not port-forwarding)

**If any of these fail, check the [Debugging Guide](../development/DEBUGGING_GUIDE.md) or ask the team.**

---

## 📖 Documentation Structure

```
docs/
├── README.md                 # ← You are here navigation hub
├── onboarding/              # New team member guides
├── development/             # Developer workflow docs  
├── roles/                   # Role-specific guides
├── architecture/            # System design docs
└── operations/              # Deployment & operations
```

**Remember**: This is a Kubernetes-first, test-driven development environment. Every change must be tested end-to-end in the actual K8s cluster.

---

*🎉 Welcome to ATS! You're now ready to build algorithmic trading infrastructure at scale.*