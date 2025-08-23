# Development Workflow

## 🚨 Critical Development Rules

**EVERY code change must follow this exact workflow:**

1. **JIRA Issue Management** - Create JIRA ticket before any work
2. **Feature Branch Development** - NEVER commit directly to main
3. **Test-Driven Development (TDD)** - Write failing test first
4. **Kubernetes-First Development** - Use K8s for all operations  
5. **End-to-End Validation** - Verify complete pipelines work
6. **Integration Testing** - Test actual service startup
7. **Pull Request Review** - Always merge through PR after review
8. **Issue Verification** - Verify resolution before closing JIRA ticket

## 🌿 Git Branching Workflow - MANDATORY

### 🚫 NEVER Commit Directly to Main Branch

**All changes must go through feature branches and pull requests:**

```bash
# ❌ WRONG - Never do this:
git checkout main
git add .
git commit -m "fix something"
git push origin main

# ✅ CORRECT - Always use feature branches:
```

### Step 1: Create Feature Branch

```bash
# Always start from latest main
git checkout main
git pull origin main

# Create descriptive feature branch
git checkout -b feature/fix-workflow-dependencies
# or
git checkout -b bugfix/resolve-gin-import-errors
# or  
git checkout -b docs/update-development-workflow
```

### Step 2: Make Changes on Feature Branch

```bash
# Make your changes (following TDD workflow below)
# Edit files, write tests, implement features

# Stage changes
git add .

# Commit with descriptive message
git commit -m "fix: add missing Python dependencies for workflow tests

- Add gin-config==0.5.0 to resolve import errors
- Add fastapi==0.115.6 for web framework support  
- Add uvicorn[standard]==0.35.0 for ASGI server
- Fix gin configuration path issues in tests

🤖 Generated with [Claude Code](https://claude.ai/code)
Co-Authored-By: Claude <noreply@anthropic.com>"
```

### Step 3: Push Feature Branch

```bash
# Push feature branch to remote
git push origin feature/fix-workflow-dependencies
```

### Step 4: Create Pull Request

```bash
# Create PR via GitHub CLI (recommended)
gh pr create --title "fix: resolve workflow dependency and configuration issues" --body "
## Summary
- Fix missing Python dependencies causing workflow failures
- Resolve gin configuration path issues in tests
- All workflow tests now pass successfully

## Changes Made
- Added gin-config, fastapi, uvicorn to requirements.txt
- Fixed test file path resolution for gin config files
- Enhanced error handling for working directory issues

## Testing
- ✅ All unit tests pass
- ✅ All integration tests pass  
- ✅ Workflow command passes: \`PYTHONPATH=src pytest tests/core/ tests/config/ tests/signals/test_indicator.py -v --tb=short --maxfail=10\`

## Verification
- [x] Tests written first (TDD followed)
- [x] Integration tests pass
- [x] End-to-end functionality verified
- [x] No breaking changes
"

# Or create PR through GitHub web interface:
# 1. Go to repository on GitHub
# 2. Click "Compare & pull request" 
# 3. Fill in title and description
# 4. Request review from team members
```

### Step 5: Review and Merge

```bash
# After PR approval, merge via GitHub interface
# GitHub will handle the merge and cleanup

# Then update your local main
git checkout main
git pull origin main

# Clean up feature branch (optional)
git branch -d feature/fix-workflow-dependencies
git push origin --delete feature/fix-workflow-dependencies
```

### Branch Naming Conventions

**Use descriptive branch names with prefixes:**

- `feature/` - New functionality
- `bugfix/` - Bug fixes  
- `docs/` - Documentation updates
- `chore/` - Maintenance tasks
- `refactor/` - Code refactoring
- `test/` - Test improvements

**Examples:**
```bash
feature/add-dataset-filtering
bugfix/fix-gin-import-errors  
docs/update-api-documentation
chore/upgrade-dependencies
refactor/simplify-analytics-service
test/improve-integration-coverage
```

### 🚨 Branch Protection Rules

**Main branch should have these protections enabled:**
- ✅ Require pull request reviews before merging
- ✅ Require status checks to pass before merging
- ✅ Require branches to be up to date before merging
- ✅ Require linear history (squash and merge)
- ✅ Do not allow bypassing the above settings

## 🎫 JIRA Issue Management - MANDATORY

### 🚫 NEVER Start Work Without JIRA Ticket

**All development work must be tracked through JIRA tickets:**

```bash
# ❌ WRONG - Never do this:
git checkout -b feature/fix-some-bug
# No JIRA ticket = No traceability = No accountability

# ✅ CORRECT - Always create JIRA ticket first:
# 1. Go to JIRA: https://your-company.atlassian.net/browse/PGPT
# 2. Create ticket: PGPT-1234
# 3. THEN create branch with ticket number
```

### Step 1: Create JIRA Ticket First

**Before any code changes:**

```bash
# 1. Identify the issue (bug, feature, technical debt)
# 2. Go to JIRA project dashboard
# 3. Click "Create Issue"
# 4. Use appropriate template from docs/templates/JIRA_TICKET_TEMPLATE.md
```

**Required JIRA Ticket Information:**
- **Summary:** Clear, actionable title
- **Description:** Detailed problem/requirement description  
- **Acceptance Criteria:** Specific, testable conditions for completion
- **Priority:** Critical/High/Medium/Low based on impact
- **Components:** Affected system areas
- **Labels:** Categorization (bug, feature, technical-debt, etc.)

### Step 2: Branch Naming with JIRA Integration

```bash
# ALWAYS include JIRA ticket number in branch name
git checkout -b PGPT-1234/fix-workflow-dependencies
git checkout -b PGPT-1235/feature-dataset-filtering  
git checkout -b PGPT-1236/docs-api-documentation
git checkout -b PGPT-1237/refactor-analytics-service
```

**Branch Naming Convention:**
```
PGPT-[ticket-number]/[type]-[brief-description]

Examples:
PGPT-1234/fix-gin-import-errors
PGPT-1235/feature-user-dashboard
PGPT-1236/docs-deployment-guide
PGPT-1237/refactor-database-layer
```

### Step 3: Commit Messages with JIRA Integration

```bash
# Include JIRA ticket number in every commit message
git commit -m "PGPT-1234: fix missing Python dependencies in workflow tests

- Add gin-config==0.5.0 to resolve import errors
- Add fastapi==0.115.6 for web framework support  
- Fix gin configuration path issues in tests
- All workflow tests now pass successfully

🤖 Generated with [Claude Code](https://claude.ai/code)
Co-Authored-By: Claude <noreply@anthropic.com>"
```

### Step 4: Pull Request Integration

**PR Title Format:**
```
PGPT-1234: Fix workflow dependency import errors
```

**PR Description Must Include:**
```markdown
## JIRA Ticket
**Link:** [PGPT-1234](https://your-company.atlassian.net/browse/PGPT-1234)

## Summary
[Brief description of changes made to address the JIRA ticket]

## JIRA Acceptance Criteria Met
- [ ] Acceptance criteria #1 - [specific requirement met]
- [ ] Acceptance criteria #2 - [specific requirement met]
- [ ] All acceptance criteria verified and ready for ticket closure

## Testing Completed  
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] End-to-end functionality verified
- [ ] Manual testing completed in dev environment

## Verification Instructions
[Step-by-step instructions for reviewers to verify the fix addresses the JIRA issue]
```

### Step 5: JIRA Status Automation

**Your GitHub Actions automatically update JIRA:**

```yaml
# .github/workflows/jira-integration.yml handles:
- PR opened → JIRA status: "In Review"  
- PR merged → JIRA status: "Done"
- Adds PR links and commit details to JIRA comments
```

**Manual JIRA Updates Required:**
- **"To Do" → "In Progress"** when starting work on ticket
- **"Code Review" → "Testing"** when PR merged (before verification)
- **"Testing" → "Done"** after production verification completed

### Step 6: Issue Verification & Closure

**🚨 NEVER close JIRA tickets without verification:**

```bash
# For Bug Fixes:
1. Reproduce original issue in dev environment  
2. Confirm fix resolves the specific problem
3. Run regression tests to prevent new issues
4. Deploy to staging/production environment
5. Verify fix works in production
6. Add verification comment to JIRA ticket
7. Confirm JIRA ticket closure

# For Features:
1. Test all acceptance criteria in dev environment
2. Verify end-to-end functionality works
3. Deploy to staging/production
4. Test feature with real user scenarios  
5. Confirm feature works for end users
6. Add verification comment to JIRA ticket
7. Confirm JIRA ticket closure
```

### JIRA Integration Examples

**Bug Report Creation:**
```bash
# Discovered: Workflow failing with import errors
# Action: Create PGPT-1234 with error details, reproduction steps
# Branch: PGPT-1234/fix-workflow-dependencies
# Commits: "PGPT-1234: fix missing gin-config dependency"
# PR: "PGPT-1234: Fix workflow dependency import errors"
# Verify: Run workflows, confirm they pass, close ticket
```

**Feature Development:**
```bash
# Request: Add dataset filtering functionality
# Action: Create PGPT-1235 with requirements, acceptance criteria
# Branch: PGPT-1235/feature-dataset-filtering
# Commits: "PGPT-1235: implement dataset filtering with search UI"
# PR: "PGPT-1235: Add comprehensive dataset filtering functionality"
# Verify: Test filtering in production, confirm works, close ticket
```

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

### 🚫 Git Workflow Violations (MOST CRITICAL)
- ❌ **Committing directly to main branch** → Always use feature branches
- ❌ **Pushing to main without PR review** → Always create pull requests
- ❌ **Merging without CI/CD checks passing** → Wait for all status checks
- ❌ **Creating PRs without proper testing** → Follow TDD workflow first
- ❌ **Bypassing branch protection rules** → Never override protections
- ❌ **Poor commit messages** → Use conventional commit format
- ❌ **Not updating branch before merge** → Always rebase/merge latest main

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
# 0. ALWAYS start with JIRA ticket (Issue Management)
# Go to JIRA: https://your-company.atlassian.net/browse/PGPT
# Create ticket: PGPT-1238 "Add recommendations API endpoint"
# Include: Summary, Description, Acceptance Criteria, Priority

# 1. THEN create feature branch with JIRA ticket (Git Workflow)  
git checkout main
git pull origin main
git checkout -b PGPT-1238/feature-recommendations-endpoint

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

# 9. Update JIRA ticket status to "In Progress"
# Go to JIRA ticket PGPT-1238, change status: "To Do" → "In Progress"

# 10. Commit and push feature branch with JIRA reference (NEVER push to main)
git add .
git commit -m "PGPT-1238: add recommendations API endpoint

- Add new /api/recommendations endpoint with CRUD operations
- Include comprehensive test coverage for all endpoints  
- Deploy with K8s configuration using existing patterns
- Verify external access works with real data
- All JIRA acceptance criteria met

🤖 Generated with [Claude Code](https://claude.ai/code)
Co-Authored-By: Claude <noreply@anthropic.com>"

git push origin PGPT-1238/feature-recommendations-endpoint

# 11. Create Pull Request with JIRA integration
gh pr create --title "PGPT-1238: Add recommendations API endpoint" --body "
## JIRA Ticket
**Link:** [PGPT-1238](https://your-company.atlassian.net/browse/PGPT-1238)

## Summary
New API endpoint for getting stock recommendations based on technical analysis.
Addresses JIRA ticket requirements for recommendation system functionality.

## JIRA Acceptance Criteria Met
- [x] Acceptance criteria #1 - API endpoint returns JSON recommendations
- [x] Acceptance criteria #2 - Endpoint supports filtering by symbol
- [x] Acceptance criteria #3 - Response includes confidence scores
- [x] Acceptance criteria #4 - Performance meets <200ms response time
- [x] All acceptance criteria verified and ready for ticket closure

## Changes Made
- Added /api/recommendations endpoint with full CRUD operations
- Comprehensive test coverage including integration tests
- Kubernetes deployment configuration using existing patterns
- External access verified with real data

## Testing Completed
- [x] Unit tests pass (TDD workflow followed)
- [x] Integration tests pass
- [x] K8s deployment successful in dev environment
- [x] External endpoint accessible (not just port-forward)
- [x] End-to-end workflow verified with real data
- [x] Performance benchmarks met
- [x] No regression issues introduced

## Verification Instructions
1. Deploy to dev environment: \`kubectl apply -f k8s/dev/\`
2. Test endpoint: \`curl http://EXTERNAL_IP:PORT/api/recommendations?symbol=AAPL\`
3. Verify response format matches JIRA acceptance criteria
4. Confirm response time <200ms as specified in JIRA ticket
5. Test error handling and edge cases
"

# 12. Wait for PR review and approval (JIRA auto-updates to "In Review")
# 13. Merge through GitHub interface (never merge locally to main)
# 14. JIRA automatically transitions to "Done" after merge

# 15. CRITICAL: Verify JIRA issue resolution in production
# Deploy to production and test the actual endpoint
curl -s "http://PRODUCTION_IP:PORT/api/recommendations?symbol=AAPL" | jq
# Verify response meets all JIRA acceptance criteria

# 16. Add verification comment to JIRA ticket
# Go to JIRA ticket PGPT-1238, add comment:
# "✅ Verified in production - all acceptance criteria met
# - Endpoint returns JSON recommendations ✓
# - Supports symbol filtering ✓  
# - Includes confidence scores ✓
# - Response time <200ms ✓
# Ready for ticket closure."

# 17. Confirm JIRA ticket is properly closed
# Ensure JIRA status: "Testing" → "Done" 

# 18. Clean up local branches
git checkout main
git pull origin main
git branch -d PGPT-1238/feature-recommendations-endpoint
```

## Development Rules Summary

**🚫 MOST CRITICAL JIRA & Git Rules:**
- 🚫 **NEVER** start work without creating JIRA ticket first
- 🚫 **NEVER** commit without JIRA ticket reference in branch/commit
- 🚫 **NEVER** close JIRA ticket without production verification
- 🚫 **NEVER** commit directly to main branch
- 🚫 **NEVER** push to main without pull request review
- 🚫 **NEVER** merge without CI/CD checks passing
- 🚫 **NEVER** bypass branch protection rules

**🚫 Development Rules:**
- 🚫 **NEVER** move to next step without verifying current step works
- 🚫 **NEVER** assume tests pass without running them
- 🚫 **NEVER** skip manual verification for user-facing changes
- 🚫 **NEVER** use kubectl directly - use dev CLI
- 🚫 **NEVER** create new infrastructure - reuse existing patterns

**✅ ALWAYS Do - JIRA & Git Workflow:**
- ✅ **ALWAYS** create JIRA ticket before starting any work
- ✅ **ALWAYS** include JIRA ticket number in branch names and commits
- ✅ **ALWAYS** create feature branch for any change  
- ✅ **ALWAYS** use JIRA ticket references in PR titles and descriptions
- ✅ **ALWAYS** create pull request for review
- ✅ **ALWAYS** wait for CI/CD checks and peer review
- ✅ **ALWAYS** merge through GitHub interface, never locally
- ✅ **ALWAYS** verify JIRA issue resolution in production before closing

**✅ ALWAYS Do - Development:**
- ✅ **ALWAYS** write test first (TDD)
- ✅ **ALWAYS** test actual functionality, not just unit tests
- ✅ **ALWAYS** verify database changes with actual queries
- ✅ **ALWAYS** confirm services start and respond correctly
- ✅ **ALWAYS** test external access, not just port-forwarding
- ✅ **ALWAYS** complete end-to-end before claiming success

---

*Remember: A feature is not complete until the entire end-to-end workflow functions with real data in the production environment. No shortcuts, no "half-baked jobs" - complete implementation only.*