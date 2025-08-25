# GitHub Branch Protection Setup Guide

## 🚨 **CRITICAL: Main Branch Protection Required**

The ATS repository currently allows direct pushes to the `main` branch, which bypasses our quality gates. This setup guide ensures all changes go through proper validation.

## 🛡️ **Required Branch Protection Rules**

### 1. Navigate to Branch Protection Settings

1. Go to your GitHub repository: `https://github.com/jianjun00/ats-genai`
2. Click **Settings** (top menu)
3. Click **Branches** (left sidebar)
4. Click **Add rule** or **Add branch protection rule**

### 2. Configure Main Branch Protection

**Branch name pattern:** `main`

#### ✅ **Required Status Checks** (CRITICAL)
Enable: `Require status checks to pass before merging`
- ☑️ `Require branches to be up to date before merging`
- ☑️ **Status checks to require:**
  - `Unit Tests (Required)` 
  - `Code Quality (Required)`
  - `protection-summary`

#### ✅ **Pull Request Requirements** (HIGHLY RECOMMENDED)
Enable: `Require a pull request before merging`
- ☑️ `Require approvals: 1` (minimum)
- ☑️ `Dismiss stale pull request approvals when new commits are pushed`
- ☑️ `Require review from code owners` (if CODEOWNERS file exists)

#### ✅ **Additional Restrictions**
- ☑️ `Restrict pushes that create files`
- ☑️ `Do not allow bypassing the above settings`
- ☑️ `Allow force pushes: DISABLED`
- ☑️ `Allow deletions: DISABLED`

## 🔄 **Recommended Development Workflow**

With branch protection enabled, the workflow becomes:

### 1. Feature Development
```bash
# Create feature branch
git checkout -b feature/your-feature-name

# Make changes and commit
git add .
git commit -m "feat: your changes"

# Push feature branch
git push origin feature/your-feature-name
```

### 2. Pull Request Process
1. Create PR from feature branch to `main`
2. **Automatic CI checks run:**
   - ✅ Unit tests must pass
   - ✅ Code quality checks must pass
   - ⚠️ Integration tests (recommended)
3. **Manual review process:**
   - Team member reviews code
   - Approves if changes look good
4. **Merge:**
   - Only possible if all required checks pass
   - GitHub automatically runs final validation

### 3. Emergency Hotfixes
For critical production issues:
```bash
# Create hotfix branch
git checkout -b hotfix/critical-issue

# Make minimal changes
git commit -m "fix: critical production issue"

# Create PR with "urgent" label
# Still requires CI checks but can be fast-tracked
```

## 🎯 **Quality Gates Enforced**

### Unit Tests (Required - Blocking)
- All core functionality tests must pass
- Minimum 70% code coverage
- Tests in: `tests/core/`, `tests/config/`, `tests/signals/`
- **Failure = PR blocked**

### Code Quality (Required - Blocking) 
- Code formatting with `black`
- Linting with `flake8`
- Security scan with `bandit`
- **Failure = PR blocked**

### Integration Tests (Recommended - Non-blocking)
- Database integration tests
- API endpoint validation
- **Failure = Warning only, doesn't block**

## 🔧 **Current CI Workflow Status**

### ✅ **Already Implemented**
- `main-branch-protection.yml` workflow
- Unit test validation
- Code quality checks
- Integration test suite
- Automated PR comments with results

### ⏳ **Needs GitHub Configuration**
- Branch protection rules (manual setup required)
- Required status checks configuration
- PR review requirements

## 🚨 **Security Benefits**

### Before Branch Protection:
```bash
# Anyone can push directly to main - DANGEROUS
git push origin main  # No validation!
```

### After Branch Protection:
```bash
# Direct pushes to main blocked
git push origin main  # ❌ REJECTED by GitHub

# Must use PR process
git push origin feature/branch  # ✅ Allowed
# → Create PR → CI validation → Review → Merge
```

## 📊 **Expected Impact**

### Code Quality Improvements:
- **100%** of changes validated before main branch
- **0** untested code in production
- **Automatic** security and quality scanning
- **Enforced** code formatting standards

### Development Process:
- **Structured** feature development workflow
- **Collaborative** code review process  
- **Documented** change history via PRs
- **Rollback-friendly** individual feature commits

## 🛠️ **Implementation Steps**

### 1. Immediate (Repository Owner)
- [ ] Set up branch protection rules (5 minutes)
- [ ] Test with a sample PR
- [ ] Notify team about new workflow

### 2. Team Transition (Next Sprint)
- [ ] Team training on new PR workflow
- [ ] Update development documentation
- [ ] Set up code owner reviews

### 3. Advanced Features (Future)
- [ ] Add automated deployment on merge
- [ ] Implement semantic release versioning
- [ ] Add performance regression testing

## 🆘 **Troubleshooting**

### "Status check required but not found"
- Wait for CI workflow to run once
- Check workflow names match exactly
- Ensure workflows trigger on `pull_request` events

### "Can't merge due to failing checks"
- Check CI workflow logs
- Fix failing tests or quality issues
- Push fixes to feature branch (triggers re-check)

### "Need admin access to set up protection"
- Repository owner needs to configure settings
- Team leads should have admin access
- Consider using GitHub teams for permissions

---

## 📋 **Quick Setup Checklist**

**Repository Owner Tasks:**
- [ ] Enable branch protection on `main`
- [ ] Require `Unit Tests (Required)` status check
- [ ] Require `Code Quality (Required)` status check
- [ ] Require PR reviews (1 minimum)
- [ ] Disable force pushes and deletions
- [ ] Test setup with sample PR

**Team Tasks:**
- [ ] Switch to feature branch workflow
- [ ] Update local development practices  
- [ ] Review and approve team PRs
- [ ] Report any workflow issues

**Validation:**
- [ ] Direct push to main is blocked
- [ ] PR with failing tests is blocked  
- [ ] PR with passing tests can be merged
- [ ] CI runs automatically on all PRs

---

**⚡ Once setup is complete, the main branch will be protected from untested changes and the team will follow a structured, quality-assured development process.**