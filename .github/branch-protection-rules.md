# GitHub Branch Protection Rules Configuration

This document provides the exact configuration needed for GitHub branch protection rules to enforce test requirements.

## Required Branch Protection Rules

### Main Branch Protection

Navigate to: **Repository Settings → Branches → Add rule**

#### Rule Configuration for `main` branch:

```yaml
Branch name pattern: main
```

**Protect matching branches:**
- ✅ Restrict pushes that create matching branches
- ✅ Restrict pushes that create matching branches  
- ✅ Require a pull request before merging
  - ✅ Require approvals: 1
  - ✅ Dismiss stale PR approvals when new commits are pushed
  - ✅ Require review from code owners (if CODEOWNERS file exists)
  - ✅ Restrict pushes that create matching branches
  - ✅ Require approval of the most recent reviewable push

**Require status checks to pass before merging:**
- ✅ Require branches to be up to date before merging
- ✅ Required status checks:
  - `CI/CD Pipeline / unit-tests`
  - `CI/CD Pipeline / integration-tests` 
  - `CI/CD Pipeline / gin-config-tests`
  - `CI/CD Pipeline / code-quality`
  - `CI/CD Pipeline / ci-success`

**Other restrictions:**
- ✅ Restrict pushes that create matching branches
- ✅ Allow force pushes: **❌ Disabled**
- ✅ Allow deletions: **❌ Disabled**

#### Advanced Settings:

- ✅ Do not allow bypassing the above settings
- ✅ Include administrators (even admins must follow rules)

### Develop Branch Protection

```yaml
Branch name pattern: develop
```

**Protect matching branches:**
- ✅ Require a pull request before merging
  - ✅ Require approvals: 1
  - ✅ Dismiss stale PR approvals when new commits are pushed

**Require status checks to pass before merging:**
- ✅ Require branches to be up to date before merging
- ✅ Required status checks:
  - `CI/CD Pipeline / unit-tests`
  - `CI/CD Pipeline / integration-tests`
  - `CI/CD Pipeline / ci-success`

**Other restrictions:**
- ✅ Allow force pushes: **❌ Disabled**
- ✅ Allow deletions: **❌ Disabled**

## GitHub CLI Commands (Alternative Setup)

You can also configure these rules using GitHub CLI:

### Install GitHub CLI
```bash
# Install GitHub CLI if not already installed
curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null
sudo apt update
sudo apt install gh

# Authenticate
gh auth login
```

### Configure Main Branch Protection
```bash
# Set up main branch protection
gh api repos/{owner}/{repo}/branches/main/protection \
  --method PUT \
  --field required_status_checks='{"strict":true,"contexts":["CI/CD Pipeline / unit-tests","CI/CD Pipeline / integration-tests","CI/CD Pipeline / gin-config-tests","CI/CD Pipeline / code-quality","CI/CD Pipeline / ci-success"]}' \
  --field enforce_admins=true \
  --field required_pull_request_reviews='{"required_approving_review_count":1,"dismiss_stale_reviews":true}' \
  --field restrictions='{"users":[],"teams":[],"apps":[]}' \
  --field allow_force_pushes=false \
  --field allow_deletions=false
```

### Configure Develop Branch Protection
```bash
# Set up develop branch protection  
gh api repos/{owner}/{repo}/branches/develop/protection \
  --method PUT \
  --field required_status_checks='{"strict":true,"contexts":["CI/CD Pipeline / unit-tests","CI/CD Pipeline / integration-tests","CI/CD Pipeline / ci-success"]}' \
  --field enforce_admins=false \
  --field required_pull_request_reviews='{"required_approving_review_count":1,"dismiss_stale_reviews":true}' \
  --field restrictions='{"users":[],"teams":[],"apps":[]}' \
  --field allow_force_pushes=false \
  --field allow_deletions=false
```

## Verification

After setting up the rules, verify they work by:

1. **Test PR Requirements:**
   ```bash
   git checkout -b test-protection
   echo "test" > test-file.txt
   git add test-file.txt
   git commit -m "test: verify branch protection"
   git push origin test-protection
   # Create PR to main - should require tests to pass
   ```

2. **Test Direct Push Rejection:**
   ```bash
   git checkout main
   echo "test" >> README.md
   git add README.md
   git commit -m "test: should be rejected"
   git push origin main
   # Should be rejected due to branch protection
   ```

## Troubleshooting

### Common Issues:

1. **Status checks not appearing:**
   - Ensure GitHub Actions workflow has run at least once
   - Check that job names in workflow match the required status checks

2. **Admin bypass not working:**
   - Check "Include administrators" setting
   - Ensure you have proper repository permissions

3. **PR not requiring reviews:**
   - Verify "Require pull request reviews" is enabled
   - Check that reviewers have appropriate permissions

### Emergency Bypass (Use Sparingly):

If you need to bypass protection in an emergency:

1. **Temporarily disable protection:**
   ```bash
   gh api repos/{owner}/{repo}/branches/main/protection --method DELETE
   # Make your emergency commit
   # Re-enable protection using commands above
   ```

2. **Use admin override:**
   - Go to specific PR
   - Click "Merge without waiting for requirements to be met"
   - Requires admin permissions and "Include administrators" to be disabled

## Best Practices

1. **Always use feature branches:**
   ```bash
   git checkout -b feature/new-feature
   # Make changes
   git push origin feature/new-feature
   # Create PR
   ```

2. **Keep PRs small and focused**
3. **Write descriptive commit messages**
4. **Ensure all tests pass locally before pushing**
5. **Request reviews from relevant team members**

## Status Check Configuration

The GitHub Actions workflow (`.github/workflows/ci.yml`) provides these status checks:

- `unit-tests`: Fast unit tests for core functionality
- `integration-tests`: Database and service integration tests  
- `gin-config-tests`: Configuration validation tests
- `code-quality`: Linting, formatting, and type checking
- `ci-success`: Overall success indicator

These align with the required status checks in the branch protection rules.