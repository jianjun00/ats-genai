# Issue Management Workflow

## 🎯 Issue Management Strategy

**Every bug, feature request, or technical debt must be tracked through JIRA tickets with full lifecycle management.**

## 🔄 Issue Lifecycle Management

### 1. Issue Discovery & Creation

**ALWAYS create JIRA tickets for:**
- 🐛 **Bugs discovered** - Any malfunction, error, or unexpected behavior
- ✨ **Feature requests** - New functionality or enhancements
- 🔧 **Technical debt** - Code refactoring, performance improvements
- 📚 **Documentation** - Missing or outdated documentation
- 🚨 **Security issues** - Vulnerabilities or security improvements
- 🏗️ **Infrastructure** - DevOps, CI/CD, deployment improvements

### 2. JIRA Ticket Creation Process

#### Step 1: Create JIRA Ticket First
```bash
# Before any code changes, create JIRA ticket
# Go to JIRA project: https://your-company.atlassian.net/browse/PGPT
# Click "Create Issue"
```

#### Step 2: JIRA Ticket Requirements
**Every JIRA ticket MUST include:**

```
Summary: [Clear, actionable title]
Description:
## Problem Statement
- What is broken or missing?
- How was this discovered?
- What is the expected behavior?

## Impact Assessment
- [ ] Critical (Production down)
- [ ] High (Major feature broken) 
- [ ] Medium (Minor functionality affected)
- [ ] Low (Enhancement/nice-to-have)

## Acceptance Criteria
- [ ] Specific, testable conditions for completion
- [ ] Definition of "done"
- [ ] Verification steps

## Technical Details (if applicable)
- Error messages/stack traces
- Affected components
- Reproduction steps
- Environment details

## Linked Issues
- Related tickets
- Dependencies
- Blockers

Labels: bug, feature, technical-debt, documentation, etc.
Priority: Critical/High/Medium/Low
Components: [affected system components]
```

#### Step 3: Assign JIRA Ticket Number
```
Example tickets:
PGPT-1234: Fix workflow dependency import errors
PGPT-1235: Add dataset filtering functionality  
PGPT-1236: Implement comprehensive logging system
PGPT-1237: Update API documentation with new endpoints
```

### 3. Git Branch Integration with JIRA

#### Branch Naming with JIRA Tickets
```bash
# ALWAYS include JIRA ticket number in branch name
git checkout -b PGPT-1234/fix-workflow-dependencies
git checkout -b PGPT-1235/feature-dataset-filtering
git checkout -b PGPT-1236/refactor-logging-system
git checkout -b PGPT-1237/docs-api-endpoints
```

#### Commit Message Integration
```bash
# Include JIRA ticket in commit messages
git commit -m "PGPT-1234: fix missing Python dependencies in workflow tests

- Add gin-config==0.5.0 to resolve import errors
- Add fastapi==0.115.6 for web framework support
- Add uvicorn[standard]==0.35.0 for ASGI server
- Fix gin configuration path issues in tests

🤖 Generated with [Claude Code](https://claude.ai/code)
Co-Authored-By: Claude <noreply@anthropic.com>"
```

### 4. Pull Request Integration

#### PR Title Format
```
PGPT-1234: Fix workflow dependency import errors
```

#### PR Description Template
```markdown
## JIRA Ticket
**Link:** [PGPT-1234](https://your-company.atlassian.net/browse/PGPT-1234)

## Summary
Brief description of changes made to address the JIRA ticket.

## Changes Made
- Specific changes implemented
- Files modified
- Dependencies added/removed

## Testing Completed
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Manual testing completed
- [ ] End-to-end workflow verified
- [ ] Performance impact assessed

## JIRA Acceptance Criteria Met
- [ ] Acceptance criteria #1 met
- [ ] Acceptance criteria #2 met
- [ ] Ready for JIRA ticket closure

## Verification Instructions
Step-by-step instructions for reviewers to verify the fix.

## Risk Assessment
- Breaking changes: Yes/No
- Performance impact: None/Low/Medium/High
- Security implications: None/Low/Medium/High
```

### 5. JIRA Ticket Status Updates

#### Automated Status Updates (via GitHub Actions)
Your existing `.github/workflows/jira-integration.yml` automatically:
- **Moves to "In Review"** when PR is opened
- **Moves to "Done"** when PR is merged
- **Adds comments** with PR links and commit details

#### Manual Status Updates Required
```bash
# Update JIRA manually for these transitions:
# PGPT-1234: "To Do" → "In Progress" (when starting work)
# PGPT-1234: "In Progress" → "Code Review" (when PR created)
# PGPT-1234: "Code Review" → "Testing" (when PR merged, before verification)
# PGPT-1234: "Testing" → "Done" (after verification completed)
```

### 6. Issue Verification & Closure

#### Verification Requirements
**NEVER close JIRA tickets until verified in target environment:**

```bash
# For bug fixes:
1. Reproduce original issue in dev environment
2. Apply fix and verify issue is resolved
3. Run regression tests
4. Deploy to staging/production
5. Verify fix works in production environment
6. Close JIRA ticket

# For features:
1. Test all acceptance criteria in dev
2. Integration test passes
3. End-to-end functionality verified
4. Deploy to staging/production  
5. Verify feature works for end users
6. Close JIRA ticket
```

#### Closure Checklist
```markdown
Before closing JIRA ticket, verify:
- [ ] All acceptance criteria met
- [ ] Code merged to main branch
- [ ] Deployed to production
- [ ] End-to-end testing completed
- [ ] No regression issues introduced
- [ ] Documentation updated (if applicable)
- [ ] Stakeholders notified (if applicable)
```

## 📊 Issue Tracking Standards

### 1. Issue Prioritization
```
Critical (P1): Production down, security vulnerability
High (P2): Major feature broken, significant user impact
Medium (P3): Minor feature issues, performance improvements
Low (P4): Enhancements, technical debt, documentation
```

### 2. Issue Categories & Labels
```
Bug: Functionality not working as expected
Feature: New functionality request
Enhancement: Improvement to existing functionality
Technical-Debt: Code refactoring, architecture improvements
Documentation: Missing or outdated documentation
Security: Security vulnerabilities or improvements
Performance: Speed, memory, or efficiency improvements
Infrastructure: DevOps, CI/CD, deployment issues
```

### 3. Required JIRA Fields
```
Summary: Clear, actionable title
Description: Detailed problem/requirement description
Priority: Critical/High/Medium/Low
Issue Type: Bug/Feature/Enhancement/Task
Components: Affected system components
Labels: Categorization tags
Assignee: Person responsible for resolution
Reporter: Person who discovered/requested issue
```

## 🚨 Critical Issue Management Rules

### NEVER Do:
- ❌ **Start coding without JIRA ticket**
- ❌ **Create PR without JIRA reference**
- ❌ **Close ticket without verification**
- ❌ **Work on unassigned tickets**
- ❌ **Skip impact assessment**
- ❌ **Merge code with failing tests**

### ALWAYS Do:
- ✅ **Create JIRA ticket first**
- ✅ **Include ticket number in branch names**
- ✅ **Reference ticket in all commits**
- ✅ **Update ticket status during workflow**
- ✅ **Verify resolution before closing**
- ✅ **Document verification steps**

## 🔄 Complete Issue Workflow Example

### Discovering and Fixing a Bug

```bash
# 1. Discover issue (e.g., workflow failing)
# Problem: GitHub Actions failing with import errors

# 2. Create JIRA ticket PGPT-1234
# Title: "Fix workflow dependency import errors"
# Include error details, reproduction steps, acceptance criteria

# 3. Create feature branch
git checkout main
git pull origin main
git checkout -b PGPT-1234/fix-workflow-dependencies

# 4. Follow TDD workflow to fix issue
# Write failing test, implement fix, verify solution

# 5. Commit with JIRA reference
git commit -m "PGPT-1234: fix missing Python dependencies in workflows"

# 6. Push and create PR
git push origin PGPT-1234/fix-workflow-dependencies
gh pr create --title "PGPT-1234: Fix workflow dependency import errors"

# 7. JIRA automatically transitions to "In Review"

# 8. After PR review and merge, JIRA transitions to "Done"

# 9. Verify fix in production environment
# Test actual workflows, confirm no regression

# 10. Add verification comment to JIRA ticket
# "Verified in production - all workflows passing"

# 11. Confirm JIRA ticket is properly closed
```

## 📈 Issue Management Metrics

### Weekly Review Questions
- How many new issues were discovered?
- What is the average time from discovery to resolution?
- How many issues are in each status?
- Are there recurring issue patterns?
- What components have the most issues?

### Quality Metrics
- Issues discovered in production vs. development
- Issues reopened due to inadequate fixes
- Time spent on each issue category
- Technical debt accumulation rate

---

**Remember: Issue management is not overhead - it's essential for maintaining code quality, tracking progress, and ensuring nothing falls through the cracks. Every issue deserves proper tracking and verification.**