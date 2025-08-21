# Development Workflow

## 🚨 Critical Development Rules

**EVERY code change must follow this exact workflow:**

1. **Test-Driven Development (TDD)** - Write failing test first
2. **Kubernetes-First Development** - Use K8s for all operations  
3. **End-to-End Validation** - Verify complete pipelines work
4. **Integration Testing** - Test actual service startup

## Test-Driven Development (TDD) - MANDATORY

### 1. Red Phase - Write Failing Test First

**Before any code change, write a test that fails:**

```bash
# Create test for new feature/bug fix
touch tests/integration/test_new_feature.py

# Write test that reproduces issue or tests new feature
cat > tests/integration/test_new_feature.py << 'EOF'
import pytest
from src.services.new_service import NewService

def test_new_service_functionality():
    service = NewService()
    result = service.new_method()
    assert result == "expected_value"
EOF

# Run test - should FAIL
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v
# ✅ Test FAILS - proves test can detect issues
```

### 2. Green Phase - Fix The Code

**Implement minimal code to make test pass:**

```bash
# Implement the feature/fix
# (edit src/services/new_service.py)

# Run test again - should PASS
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v
# ✅ Test PASSES - feature works
```

### 3. Refactor Phase - Clean Up

```bash
# Clean up code while keeping tests passing
# Run full test suite to prevent regressions
PYTHONPATH=src pytest tests/ -v --tb=short
# ✅ All tests still pass
```

### 4. Integration Verification

**Test actual functionality in real environment:**

```bash
# Test actual service startup (not just unit tests)
PYTHONPATH=src pytest tests/integration/test_analytics_platform_integration.py::TestAnalyticsPlatformIntegration::test_backend_api_can_start -v

# Test database connectivity (catches auth issues)
PYTHONPATH=src pytest tests/integration/test_analytics_platform_integration.py::TestRealWorldScenarios::test_database_connectivity -v

# Test external endpoints actually work
curl -s "http://external-ip:port/api/health" | jq
```

## Kubernetes-First Development

### Always Use Dev CLI

**❌ NEVER use kubectl directly for development**  
**✅ ALWAYS use dev CLI for all operations**

```bash
# Database operations
python scripts/dev_cli.py query "SELECT COUNT(*) FROM dev_daily_prices"
python scripts/dev_cli.py query "SELECT * FROM dev_instruments LIMIT 5"

# Job management
python scripts/dev_cli.py job price-unification --symbols AAPL,MSFT --date 2024-01-15
python scripts/dev_cli.py list
python scripts/dev_cli.py logs job-name

# Database migrations
python scripts/dev_cli.py migrate price-unification
```

### Environment Variables Are Pre-Configured

**❌ NEVER manually set environment variables:**
```bash
# DON'T DO THIS:
PYTHONPATH=src ENVIRONMENT=dev DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=dev_password DB_NAME=dev_db python script.py
```

**✅ Environment variables are automatically configured in K8s:**
- All scripts work with existing infrastructure
- ConfigMaps/Secrets handle all configuration
- Just run scripts directly

### Use Existing Infrastructure Patterns

**Before creating new deployments:**

```bash
# Check existing infrastructure first
kubectl get all -n ats-dev
kubectl get configmaps -n ats-dev

# Copy successful patterns
kubectl get configmap working-analytics-webapp-config -o yaml > base-config.yaml
# Modify base-config.yaml minimally
kubectl create configmap new-webapp-config --from-file=webapp.py=new_webapp.py
```

## End-to-End Development Checklist

**EVERY feature must be complete end-to-end:**

### 1. Real Data Generation
```bash
# Generate actual data using real systems
python scripts/dev_cli.py enhanced-training --symbol TSLA --days-back 120
```

### 2. Database Verification
```bash
# Verify data exists in database
python scripts/dev_cli.py query "SELECT COUNT(*) FROM dev_training_datasets WHERE dataset_name LIKE 'enhanced_%'"

# Check data structure and metadata
python scripts/dev_cli.py query "SELECT dataset_name, total_sequences, feature_count FROM dev_training_datasets ORDER BY id DESC LIMIT 5"
```

### 3. API Testing
```bash
# Test all endpoints with real data (not mock data)
curl -s "http://external-ip:nodeport/api/datasets" | jq
curl -s "http://external-ip:nodeport/api/distributions/2" | jq
curl -s "http://external-ip:nodeport/api/ohlc/2" | jq
```

### 4. Frontend Verification
- Open actual web application URL in browser
- Test all interactive features (filtering, charting, table view)
- Verify real data displays correctly (not placeholder text)
- Check that all tabs and features function properly

### 5. Complete System Integration
- Data generation → Database storage → API retrieval → Web visualization
- No broken links in the chain
- All components work with real production-like data

## Testing Standards

### Test Types & Commands

```bash
# Unit tests
PYTHONPATH=src pytest tests/unit/ -v

# Integration tests (CRITICAL)
PYTHONPATH=src pytest tests/integration/ -v

# Database tests
PYTHONPATH=src pytest tests/ -m database -v

# Specific functionality
PYTHONPATH=src pytest tests/specific_feature/ -v --tb=short
```

### Integration Test Examples

**Test actual service startup:**
```bash
PYTHONPATH=src pytest tests/integration/test_analytics_platform_integration.py::TestAnalyticsPlatformIntegration::test_backend_api_can_start -v
```

**Test database connectivity:**
```bash
PYTHONPATH=src pytest tests/integration/test_analytics_platform_integration.py::TestRealWorldScenarios::test_database_connectivity -v
```

**Test frontend dependencies:**
```bash
PYTHONPATH=src pytest tests/integration/test_analytics_platform_integration.py::TestAnalyticsPlatformIntegration::test_frontend_dependencies_can_install -v
```

### External Access Testing

**NEVER test only via port-forwarding - test external access:**

```bash
# Get actual external URL
kubectl get nodes -o wide
kubectl get service service-name -n namespace

# Test external URL (not localhost)
curl -s "http://EXTERNAL_IP:NODEPORT/health"

# Check what users actually see
curl -s "http://EXTERNAL_IP:NODEPORT/" | grep -i localhost
```

## Critical Anti-Patterns to Avoid

### Half-Baked Development
- ❌ **Unit tests pass but service doesn't start** → Not end-to-end
- ❌ **API returns mock data but real data fails** → Not end-to-end
- ❌ **Frontend works locally but not in Kubernetes** → Not end-to-end  
- ❌ **Database migration works but data generation fails** → Not end-to-end
- ❌ **Individual components work but integration fails** → Not end-to-end

### Infrastructure Mistakes
- ❌ **Using kubectl directly** → Use dev CLI instead
- ❌ **Setting environment variables manually** → Use existing K8s config
- ❌ **Creating new deployment patterns** → Reuse existing patterns
- ❌ **Installing packages in K8s jobs** → Use base Docker images
- ❌ **Testing only via port-forward** → Test external access

### Testing Shortcuts
- ❌ **Claiming functionality works without tests** → Always write tests first
- ❌ **Writing tests after code** → TDD requires tests first
- ❌ **Skipping integration tests** → Integration tests are mandatory
- ❌ **Not testing actual service startup** → Test real functionality

## Step-by-Step Workflow Example

### Implementing New API Endpoint

```bash
# 1. Write failing test first (TDD Red Phase)
touch tests/api/test_recommendations_endpoint.py
# Write test that calls new endpoint - should fail

# 2. Run test - verify it fails
PYTHONPATH=src pytest tests/api/test_recommendations_endpoint.py -v
# ✅ Test fails - proves test works

# 3. Implement minimal endpoint (TDD Green Phase)  
# Edit src/api/endpoints/recommendations.py

# 4. Run test - verify it passes
PYTHONPATH=src pytest tests/api/test_recommendations_endpoint.py -v
# ✅ Test passes - endpoint works

# 5. Integration testing
PYTHONPATH=src pytest tests/integration/ -v
# ✅ All integration tests pass

# 6. Deploy to K8s using existing patterns
kubectl get configmap existing-api-config -o yaml > base-config.yaml
# Modify base-config.yaml with new endpoint
kubectl apply -f modified-deployment.yaml

# 7. Test external access
curl -s "http://EXTERNAL_IP:NODEPORT/api/recommendations" | jq
# ✅ External endpoint works

# 8. Verify in browser (if applicable)
# Open http://EXTERNAL_IP:NODEPORT/api/recommendations
# ✅ Browser shows expected response
```

## Development Rules Summary

**Critical Rules:**
- 🚫 **NEVER** move to next step without verifying current step works
- 🚫 **NEVER** assume tests pass without running them
- 🚫 **NEVER** skip manual verification for user-facing changes
- 🚫 **NEVER** use kubectl directly - use dev CLI
- 🚫 **NEVER** create new infrastructure - reuse existing patterns

**Always Do:**
- ✅ **ALWAYS** write test first (TDD)
- ✅ **ALWAYS** test actual functionality, not just unit tests
- ✅ **ALWAYS** verify database changes with actual queries
- ✅ **ALWAYS** confirm services start and respond correctly
- ✅ **ALWAYS** test external access, not just port-forwarding
- ✅ **ALWAYS** complete end-to-end before claiming success

---

*Remember: A feature is not complete until the entire end-to-end workflow functions with real data in the production environment. No shortcuts, no "half-baked jobs" - complete implementation only.*