# JIRA Issue Creation Guide

## 🎯 When to Create JIRA Issues

**Create JIRA tickets for EVERY:**
- 🐛 Bug discovered (any error, malfunction, unexpected behavior)
- ✨ Feature request (new functionality, enhancement)
- 🔧 Technical debt (refactoring, performance, code quality)
- 📚 Documentation (missing, outdated, unclear docs)
- 🚨 Security issue (vulnerability, security improvement)
- 🏗️ Infrastructure task (DevOps, CI/CD, deployment)

## 📋 Step-by-Step JIRA Creation Process

### Method 1: JIRA Web Interface (Recommended)

#### Step 1: Access JIRA Project
```
1. Go to: https://your-company.atlassian.net
2. Navigate to Projects → PGPT
3. Or direct link: https://your-company.atlassian.net/browse/PGPT
```

#### Step 2: Create New Issue
```
1. Click "Create" button (+ icon) in top navigation
2. Or press "C" keyboard shortcut from anywhere in JIRA
3. JIRA issue creation form will open
```

#### Step 3: Fill Required Fields
```
Project: PGPT (should be pre-selected)
Issue Type: 
  - Bug: Something is broken or not working correctly
  - Story: New feature or user functionality  
  - Task: General work item (technical debt, docs, etc.)
  - Epic: Large feature spanning multiple stories

Summary: [Clear, actionable title - becomes PGPT-XXXX]
  Examples:
  - "Fix workflow dependency import errors"
  - "Add dataset filtering functionality to analytics dashboard"
  - "Update deployment documentation with K8s examples"

Description: [Use templates below - copy/paste and fill in]
```

### Method 2: JIRA CLI (For Power Users)

#### Installation
```bash
# Option 1: Node.js CLI
npm install -g jira-cli

# Option 2: Python CLI  
pip install jira

# Option 3: Go CLI
go install github.com/ankitpokhrel/jira-cli/cmd/jira@latest
```

#### Configuration
```bash
# Configure JIRA CLI with your credentials
jira config

# You'll need:
# - JIRA URL: https://your-company.atlassian.net
# - Email: your-email@company.com
# - API Token: (generate from JIRA Account Settings → Security → API tokens)
```

#### Create Issues via CLI
```bash
# Basic bug creation
jira create --project PGPT --type Bug \
  --summary "Fix workflow dependency import errors" \
  --priority High \
  --description "Workflows failing with ModuleNotFoundError: No module named 'gin'"

# Feature request creation  
jira create --project PGPT --type Story \
  --summary "Add dataset filtering functionality" \
  --priority Medium \
  --description "Users need ability to filter datasets by symbol, date range, and data quality"

# Technical debt task
jira create --project PGPT --type Task \
  --summary "Refactor analytics service for better performance" \
  --priority Low \
  --labels "technical-debt,performance"
```

### Method 3: Integration with Development Tools

#### VS Code Extension
```bash
# Install JIRA VS Code extension
# Search: "Atlassian for VS Code" in extensions marketplace
# Configure with JIRA credentials
# Create issues directly from VS Code
```

#### GitHub Issue to JIRA (if enabled)
```markdown
# Create GitHub issue with special formatting
# GitHub Actions can auto-create JIRA tickets

Title: [JIRA] Fix workflow dependency errors
Body: 
@jira-bot create ticket
Type: Bug  
Priority: High
Component: CI/CD
```

## 📝 JIRA Issue Templates

### 🐛 Bug Report Template
Copy this template into JIRA Description field:

```markdown
## Problem Statement
**What is broken?**
- Specific functionality that is not working
- Error messages encountered
- Expected vs. actual behavior

**How was this discovered?**
- User report, testing, monitoring, etc.
- Environment where discovered (dev/staging/prod)

**When did this start?**
- Recent deployment? Specific commit? Always been broken?

## Reproduction Steps
1. Step-by-step instructions to reproduce the issue
2. Include specific inputs, configurations, or conditions
3. Note any environment-specific requirements

## Impact Assessment
- [ ] Production users affected
- [ ] Critical business functionality broken
- [ ] Performance degradation
- [ ] Data integrity issues
- [ ] Security implications

**Affected Users:** [Number/percentage of users impacted]
**Business Impact:** [Revenue, reputation, compliance implications]

## Technical Details
**Error Messages:**
```
[Full error messages, stack traces, logs]
```

**Environment:**
- Operating System:
- Browser/Client version:  
- Database version:
- Kubernetes namespace:
- Recent deployments:

**Affected Components:**
- [ ] Frontend (React/web interface)
- [ ] Backend API
- [ ] Database
- [ ] CI/CD pipelines
- [ ] Infrastructure/K8s
- [ ] External integrations

## Acceptance Criteria
- [ ] Issue can be reproduced consistently
- [ ] Root cause identified
- [ ] Fix implemented and tested
- [ ] Regression tests added
- [ ] No new issues introduced
- [ ] Verified in production environment

## Related Issues
- Duplicates: 
- Related bugs:
- Dependent issues:
```

### ✨ Feature Request Template

```markdown
## Business Justification
**Why is this needed?**
- Business value or user benefit
- Problem this feature solves
- Strategic importance

**Who requested this?**
- Stakeholder name and role
- User persona or target audience

## Feature Description
**What should the feature do?**
- High-level functional description
- Key capabilities and behaviors
- Integration points with existing system

**User Story:**
As a [user type], I want [functionality] so that [benefit/goal].

## Acceptance Criteria
**Functional Requirements:**
- [ ] Specific feature behavior #1
- [ ] Specific feature behavior #2
- [ ] Error handling scenarios
- [ ] Data validation requirements
- [ ] Performance requirements

**Non-Functional Requirements:**
- [ ] Performance benchmarks (response times, throughput)
- [ ] Security requirements
- [ ] Scalability considerations
- [ ] Accessibility standards
- [ ] Mobile responsiveness (if applicable)

## Technical Considerations
**Implementation Approach:**
- Proposed technical solution
- Architecture changes needed
- Database schema modifications
- API changes required

**Dependencies:**
- Required infrastructure changes
- Third-party integrations
- Other features or tickets

**Risk Assessment:**
- Technical complexity: [Low/Medium/High]
- Breaking changes: [Yes/No]
- Performance impact: [None/Low/Medium/High]

## Design Requirements
- [ ] UI/UX mockups needed
- [ ] API specification required
- [ ] Database design changes
- [ ] Documentation updates

## Success Metrics
- How will we measure feature success?
- Key performance indicators
- User adoption targets
- Business metrics impact

## Related Issues
- Dependencies:
- Related features:
- Documentation tickets:
```

### 🔧 Technical Debt Template

```markdown
## Current State Problem
**What technical debt exists?**
- Code smells, outdated patterns, or architectural issues
- Performance bottlenecks
- Maintenance difficulties
- Security vulnerabilities

**Why does this matter?**
- Impact on development velocity
- Risk to system stability
- Cost of maintenance
- Future feature delivery constraints

## Proposed Solution
**What needs to be changed?**
- Specific refactoring or improvements
- Architecture changes
- Code cleanup areas
- Dependency updates

**Technical Approach:**
- Implementation strategy
- Migration plan (if applicable)
- Testing strategy
- Rollback plan

## Benefits
**Development Benefits:**
- Faster feature development
- Easier maintenance
- Better code readability
- Improved testing

**System Benefits:**
- Better performance
- Improved reliability
- Enhanced security
- Better scalability

## Acceptance Criteria
- [ ] Code refactoring completed
- [ ] All existing functionality preserved
- [ ] Performance improved or maintained
- [ ] Test coverage maintained or improved
- [ ] Documentation updated
- [ ] No regression issues introduced

## Risk Assessment
- [ ] Breaking changes required: [Yes/No]
- [ ] Database migrations needed: [Yes/No]
- [ ] Downtime required: [Yes/No]
- [ ] External API changes: [Yes/No]

## Effort Estimation
- Development effort: [hours/days]
- Testing effort: [hours/days]
- Documentation effort: [hours/days]
- Total estimated effort: [hours/days]
```

## 🔄 JIRA Issue Lifecycle Management

### Issue Status Flow
```
To Do → In Progress → Code Review → Testing → Done
```

### Status Transitions
```bash
# Manual transitions required:
1. "To Do" → "In Progress" (when starting work)
2. "Code Review" → "Testing" (after PR merge, before verification)
3. "Testing" → "Done" (after production verification)

# Automatic transitions (via GitHub Actions):
- PR opened → "In Review"
- PR merged → "Done" (but should manually move to "Testing" first)
```

### Issue Fields Management
```
Priority: Critical > High > Medium > Low
Components: Frontend, Backend, Database, CI/CD, Infrastructure
Labels: bug, feature, technical-debt, documentation, security, performance
Epic Link: [If part of larger feature]
Story Points: [For estimation - optional]
```

## 🚨 Critical JIRA Creation Rules

### NEVER Create Issues Without:
- ❌ Clear, actionable summary
- ❌ Detailed problem description
- ❌ Specific acceptance criteria
- ❌ Appropriate priority assignment
- ❌ Affected component identification

### ALWAYS Include:
- ✅ Problem statement or requirement description
- ✅ Step-by-step reproduction (for bugs) or user stories (for features)
- ✅ Clear acceptance criteria for completion
- ✅ Impact assessment (users affected, business impact)
- ✅ Technical details (error messages, environment, affected components)
- ✅ Related issues or dependencies

## 🔗 JIRA Integration with Development

### Branch Creation with JIRA Reference
```bash
# After creating JIRA ticket PGPT-1234
git checkout main
git pull origin main
git checkout -b PGPT-1234/fix-workflow-dependencies
```

### Commit Messages with JIRA Reference
```bash
git commit -m "PGPT-1234: fix missing Python dependencies in workflow tests

- Add gin-config==0.5.0 to resolve import errors
- Add fastapi==0.115.6 for web framework support
- Fix gin configuration path issues in tests

🤖 Generated with [Claude Code](https://claude.ai/code)
Co-Authored-By: Claude <noreply@anthropic.com>"
```

### Pull Request Integration
```markdown
# PR Title: PGPT-1234: Fix workflow dependency import errors

## JIRA Ticket
**Link:** [PGPT-1234](https://your-company.atlassian.net/browse/PGPT-1234)

## JIRA Acceptance Criteria Met
- [x] Acceptance criteria #1 - Workflows can import gin module
- [x] Acceptance criteria #2 - All tests pass successfully
- [x] Acceptance criteria #3 - No regression issues introduced
```

## 📊 JIRA Issue Quality Checklist

### Before Creating JIRA Ticket:
- [ ] Is this actually an issue that needs tracking?
- [ ] Have I searched for existing similar tickets?
- [ ] Do I have enough information to write a clear description?
- [ ] Have I identified the appropriate priority and components?

### JIRA Ticket Quality Check:
- [ ] Clear, actionable summary (becomes the ticket title)
- [ ] Detailed problem description or requirement
- [ ] Specific acceptance criteria (how we know it's done)
- [ ] Appropriate priority based on business impact
- [ ] Correct components and labels for categorization
- [ ] Technical details included (for bugs)
- [ ] Business justification included (for features)

### Before Closing JIRA Ticket:
- [ ] All acceptance criteria met and verified
- [ ] Code changes tested and deployed to production
- [ ] Production verification completed successfully
- [ ] No regression issues introduced
- [ ] Documentation updated (if applicable)
- [ ] Stakeholders notified of resolution (if applicable)

---

## 🚀 Quick Start Examples

### Example 1: Bug Discovery to JIRA Creation
```bash
# 1. Discover bug: "GitHub Actions workflow failing with import error"
# 2. Go to JIRA → Create Issue
# 3. Fill form:
#    Summary: "Fix workflow dependency import errors"
#    Type: Bug
#    Priority: High
#    Description: [Use bug template above]
# 4. Submit → Get ticket number: PGPT-1234
# 5. Create branch: git checkout -b PGPT-1234/fix-workflow-dependencies
```

### Example 2: Feature Request to JIRA Creation
```bash
# 1. Receive feature request: "Add dataset filtering to dashboard"
# 2. Go to JIRA → Create Issue  
# 3. Fill form:
#    Summary: "Add dataset filtering functionality to analytics dashboard"
#    Type: Story
#    Priority: Medium
#    Description: [Use feature template above]
# 4. Submit → Get ticket number: PGPT-1235
# 5. Create branch: git checkout -b PGPT-1235/feature-dataset-filtering
```

Remember: **Every piece of development work starts with a JIRA ticket!** No code changes without proper issue tracking and acceptance criteria.