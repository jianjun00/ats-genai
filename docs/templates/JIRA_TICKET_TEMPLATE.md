# JIRA Ticket Templates

## 🐛 Bug Report Template

```
Summary: [Concise description of the bug]

Issue Type: Bug
Priority: [Critical/High/Medium/Low]
Components: [Affected system components]
Labels: bug, [additional relevant labels]

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

## ✨ Feature Request Template

```
Summary: [Clear description of the requested feature]

Issue Type: Story/Feature
Priority: [High/Medium/Low]
Components: [System components that will be affected]
Labels: feature, enhancement, [domain-specific labels]

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

## 🔧 Technical Debt Template

```
Summary: [Description of technical debt to address]

Issue Type: Technical Task
Priority: [Medium/Low] (rarely Critical/High)
Components: [Code areas affected]
Labels: technical-debt, refactoring, [specific area]

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

## 🚨 Critical/Urgent Issue Template

```
Summary: [CRITICAL] [Brief description of urgent issue]

Issue Type: Bug
Priority: Critical
Components: [Affected systems]
Labels: critical, urgent, production

## 🚨 CRITICAL ISSUE ALERT 🚨

**Production Impact:** [Describe immediate user/business impact]
**Discovery Time:** [When was this discovered]
**Reporter:** [Who found this issue]
**Current Status:** [System status - down/degraded/at-risk]

## Immediate Actions Taken
- [ ] Incident response team notified
- [ ] Stakeholders alerted
- [ ] Monitoring dashboards checked
- [ ] Initial diagnosis completed
- [ ] Workaround implemented (if available)

## Problem Description
**What is broken?**
- Specific failure mode
- Systems affected
- User impact

**Error Evidence:**
```
[Logs, error messages, monitoring alerts]
```

## Impact Assessment
**Users Affected:** [Specific numbers/percentages]
**Business Functions Down:**
- [ ] User authentication
- [ ] Data processing
- [ ] API endpoints
- [ ] Payment processing
- [ ] Other critical functions

**Financial Impact:** [Revenue loss, SLA violations]

## Root Cause Investigation
**Initial Hypothesis:**
- Suspected root cause
- Recent changes that might be related
- Environmental factors

**Investigation Steps:**
1. [ ] Check recent deployments
2. [ ] Review system logs
3. [ ] Verify database connectivity
4. [ ] Check external service dependencies
5. [ ] Examine resource utilization

## Resolution Plan
**Immediate Actions (0-30 minutes):**
- [ ] Action 1
- [ ] Action 2

**Short-term Fix (30 minutes - 2 hours):**
- [ ] Action 1
- [ ] Action 2

**Long-term Resolution (post-incident):**
- [ ] Root cause fix
- [ ] Prevention measures
- [ ] Process improvements

## Communication Plan
- [ ] Status page updated
- [ ] Customer support notified
- [ ] Executive team alerted
- [ ] Regular updates scheduled

## Post-Incident Requirements
- [ ] Full root cause analysis
- [ ] Post-mortem meeting scheduled
- [ ] Prevention measures implemented
- [ ] Process improvements documented
- [ ] Incident response review
```

---

## 📋 Quick Reference Checklist

### Before Creating Any JIRA Ticket:
- [ ] Is this actually an issue that needs tracking?
- [ ] Have I searched for existing similar tickets?
- [ ] Do I have enough information to write a clear description?
- [ ] Have I identified the appropriate priority and components?

### JIRA Ticket Quality Check:
- [ ] Clear, actionable summary
- [ ] Detailed problem description
- [ ] Specific acceptance criteria
- [ ] Appropriate priority and labels
- [ ] Technical details included (for bugs)
- [ ] Business justification included (for features)

### Before Closing JIRA Ticket:
- [ ] All acceptance criteria met
- [ ] Code changes tested and verified
- [ ] Deployed to production (if applicable)
- [ ] No regression issues introduced
- [ ] Documentation updated
- [ ] Stakeholders notified