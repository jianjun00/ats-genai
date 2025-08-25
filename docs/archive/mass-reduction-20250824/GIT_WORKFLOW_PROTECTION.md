# Git Workflow and Branch Protection

This document describes the comprehensive git workflow and protection rules that enforce 100% test success before commits to master/main branch.

## 🛡️ Protection Layers

### Layer 1: Local Git Hooks
- **Pre-commit Hook**: Validates tests before allowing commits
- **Pre-push Hook**: Enforces comprehensive testing for master/main branches
- **Security Checks**: Scans for sensitive information and credentials

### Layer 2: GitHub Branch Protection
- **Required Status Checks**: All CI/CD tests must pass
- **Pull Request Reviews**: Minimum 1 approval required
- **Force Push Protection**: Prevents force pushes to protected branches

### Layer 3: CI/CD Pipeline
- **Parallel Test Execution**: Unit, integration, config, and quality checks
- **Comprehensive Coverage**: All test categories must pass
- **Automated Enforcement**: No human bypass possible

## 🚀 Quick Setup

### For New Team Members:
```bash
# 1. Clone the repository
git clone <repository-url>
cd <repository-name>

# 2. Install git hooks
./scripts/setup_git_hooks.sh

# 3. Verify setup
echo "test" > test.txt
git add test.txt
git commit -m "test: verify hooks working"
# Should run tests automatically

# 4. Clean up test
git reset --hard HEAD~1
rm test.txt
```

### For Repository Administrators:
1. **Set up GitHub branch protection** (see `.github/branch-protection-rules.md`)
2. **Ensure CI/CD workflow is active** (`.github/workflows/ci.yml`)
3. **Verify all team members run setup script**

## 📋 Workflow Rules

### ✅ Allowed Operations

#### Feature Branch Development:
```bash
# Create feature branch
git checkout -b feature/new-feature

# Make changes and commit (runs fast tests)
git add .
git commit -m "feat: add new feature"

# Push to feature branch (basic validation)
git push origin feature/new-feature

# Create PR (triggers full CI/CD)
# PR will be blocked until all tests pass
```

#### Hotfix Workflow:
```bash
# Create hotfix branch
git checkout -b hotfix/urgent-fix

# Make minimal fix
git add .
git commit -m "fix: urgent production issue"

# Push and create PR
git push origin hotfix/urgent-fix
# Still requires tests to pass before merging
```

### ❌ Blocked Operations

#### Direct Commits to Master/Main:
```bash
# This will be BLOCKED
git checkout main
git add .
git commit -m "direct commit"
# Pre-commit hook runs comprehensive tests

git push origin main
# Pre-push hook blocks push to protected branch
```

#### Force Pushes to Protected Branches:
```bash
# This will be BLOCKED by GitHub
git push --force origin main
# Error: force pushes disabled on protected branch
```

#### Commits with Failing Tests:
```bash
# This will be BLOCKED
git add broken_code.py
git commit -m "broken feature"
# Pre-commit hook fails, commit rejected
```

## 🔧 Test Requirements by Branch

### Feature Branches:
- **Fast Unit Tests**: Core functionality must work
- **Basic Validation**: Smoke tests only
- **Security Checks**: No sensitive data
- **Time Limit**: ~2 minutes

### Main/Master Branch:
- **Comprehensive Tests**: All test categories
- **Integration Tests**: Full system validation  
- **Configuration Tests**: All gin config scenarios
- **Quality Checks**: Code standards and security
- **Time Limit**: ~10 minutes

### Pull Requests:
- **All CI/CD Tests**: Must pass completely
- **Review Required**: Minimum 1 approval
- **Branch Up-to-Date**: Must rebase on latest main
- **Status Checks**: All required checks green

## 🚨 Emergency Procedures

### Bypass Hooks (Use Sparingly):
```bash
# Bypass pre-commit hook
git commit --no-verify

# Bypass pre-push hook  
git push --no-verify

# Bypass with environment variable
NO_VERIFY=1 git commit
NO_VERIFY=1 git push
```

### Emergency Main Branch Access:
1. **Disable branch protection temporarily** (admin only)
2. **Make emergency commit**
3. **Re-enable protection immediately**
4. **Create follow-up PR to fix properly**

### GitHub Admin Override:
1. **Go to specific PR**
2. **Click "Merge without waiting for requirements"**
3. **Requires admin permissions**
4. **Log emergency reason in PR**

## 📊 Test Categories and Requirements

### Fast Tests (Pre-commit):
```bash
# Core functionality - must complete in <2 minutes
python -m pytest tests/core/ tests/config/test_logging_config.py tests/signals/test_indicator.py
```

### Comprehensive Tests (Pre-push to main):
```bash
# All categories - must complete in <10 minutes
python -m pytest tests/core/ tests/config/ tests/calendars/ tests/signals/
```

### CI/CD Tests (Pull Requests):
```bash
# Parallel execution across categories
unit-tests:      tests marked with @pytest.mark.unit
integration-tests: tests marked with @pytest.mark.integration  
gin-config-tests: tests marked with @pytest.mark.gin_heavy
code-quality:    linting, formatting, type checking
```

## 🛠️ Troubleshooting

### Common Issues:

#### "Tests failed, commit rejected":
```bash
# Check which tests failed
./scripts/test_commands.sh unit

# Fix the issues
# Try commit again
git commit -m "fix: resolve test failures"
```

#### "Push to main blocked":
```bash
# Use feature branch instead
git checkout -b feature/my-changes
git push origin feature/my-changes

# Create PR for review
# Merge through GitHub after tests pass
```

#### "GitHub status checks not passing":
```bash
# Check GitHub Actions tab
# Look for failed workflow runs
# Fix issues and push again

# If urgent, admin can override (not recommended)
```

#### "Hook not running":
```bash
# Reinstall hooks
./scripts/setup_git_hooks.sh

# Check if hooks are executable
ls -la .git/hooks/

# Test hook manually
.git/hooks/pre-commit
```

### Debug Commands:

```bash
# Check current branch protection
git branch -r

# Test hooks manually
bash .git/hooks/pre-commit
bash .git/hooks/pre-push < /dev/null

# Run tests manually
./scripts/test_commands.sh all

# Check git configuration
git config --list | grep hook
```

## 📈 Benefits

### Code Quality:
- **100% Test Coverage**: All code tested before merge
- **Consistent Standards**: Automated quality enforcement
- **Early Detection**: Issues caught at commit time
- **Security**: Automatic credential scanning

### Team Productivity:
- **Faster Reviews**: Tests verify functionality
- **Reduced Bugs**: Issues caught before deployment  
- **Consistent Process**: Same rules for everyone
- **Clear Feedback**: Immediate test results

### Risk Reduction:
- **Protected Main**: Cannot break production branch
- **Validated Changes**: All changes tested comprehensively
- **Audit Trail**: Complete history of test results
- **Emergency Procedures**: Clear escalation paths

## 🔄 Continuous Improvement

### Monitoring:
- **Track test execution times**
- **Monitor bypass usage**
- **Review failed test patterns**
- **Update rules based on team feedback**

### Updates:
- **Hook improvements**: Update shared templates
- **New test categories**: Add to CI/CD pipeline
- **Process refinements**: Based on team experience
- **Documentation**: Keep current with changes

This comprehensive protection system ensures that the master/main branch maintains 100% test success while providing clear processes for development and emergency situations.