# Jira Integration Setup Guide
**Portfolio GPT MVP - Project Management Integration**

## Overview
Set up Jira integration with GitHub to provide enterprise-grade project tracking while maintaining developer workflow efficiency.

## Jira Project Configuration

### 1. Create Jira Project
**Project Details:**
- **Project Name**: Portfolio GPT MVP
- **Project Key**: PGPT
- **Project Type**: Software Development
- **Template**: Scrum

### 2. Configure Issue Types
**Epic**: Major feature areas
```
PGPT-1: Core Recommendation Engine
PGPT-2: Data Pipeline Infrastructure  
PGPT-3: Authentication & Subscription System
PGPT-4: Dashboard & Visualization
PGPT-5: Model Training Pipeline
```

**Story**: User stories and features
```
PGPT-10: Hourly Forecast Generation
PGPT-11: Recommendation Logic Implementation
PGPT-12: API Authentication System
PGPT-13: Dashboard Recommendation Grid
```

**Task**: Development tasks
```
PGPT-20: Set up database schema for forecasts
PGPT-21: Implement Polygon API integration
PGPT-22: Create React dashboard components
```

**Bug**: Defects and issues
```
PGPT-30: API response time exceeds 200ms threshold
PGPT-31: Forecast accuracy below 55% baseline
```

### 3. Custom Fields
Add Portfolio GPT specific fields:
- **Data Source**: Polygon, Tiingo, Unified
- **Stock Universe**: Free Tier (5), Premium Tier (3000)
- **Environment**: dev, intg, prod
- **Model Version**: v1.0, v1.1, etc.
- **Confidence Score**: 0-100% for ML features

### 4. Workflow Configuration
**Development Workflow:**
```
TO DO → IN PROGRESS → CODE REVIEW → TESTING → DONE
```

**Data Pipeline Workflow:**
```
BACKLOG → DEVELOPMENT → VALIDATION → INTEGRATION → PRODUCTION
```

## GitHub-Jira Integration

### 1. Install Jira GitHub Integration
**GitHub App Installation:**
1. Go to GitHub repository Settings
2. Navigate to Integrations & services
3. Install "Jira Software Cloud" app
4. Authorize access to repository

### 2. Configure Automation Rules
**Smart Commits Integration:**
Enable automatic Jira updates from Git commits:

```bash
# Commit message format that updates Jira
git commit -m "PGPT-20: Add forecast database schema

- Create forecasts table with OHLCV data
- Add indexes for time-series queries
- Include confidence score tracking

#time 2h #comment Database schema ready for testing"
```

**Automation Rules:**
- **Branch Creation**: Auto-transition to "In Progress" when branch created
- **Pull Request**: Auto-transition to "Code Review" when PR opened
- **Merge**: Auto-transition to "Done" when PR merged
- **Issue Creation**: Auto-create GitHub issue when Jira story created

### 3. GitHub Actions for Jira Integration
Create workflow for automatic Jira updates:

```yaml
# .github/workflows/jira-integration.yml
name: Jira Integration

on:
  pull_request:
    types: [opened, closed]
  issues:
    types: [opened, closed]

jobs:
  update-jira:
    runs-on: ubuntu-latest
    steps:
      - name: Extract Jira Issue Key
        id: jira
        run: |
          # Extract PGPT-XXX from branch name or PR title
          echo "issue_key=$(echo '${{ github.head_ref }}' | grep -o 'PGPT-[0-9]\+')" >> $GITHUB_OUTPUT
      
      - name: Transition Jira Issue
        if: steps.jira.outputs.issue_key
        uses: atlassian/gajira-transition@master
        with:
          issue: ${{ steps.jira.outputs.issue_key }}
          transition: ${{ github.event.action == 'opened' && 'In Progress' || 'Done' }}
        env:
          JIRA_BASE_URL: ${{ secrets.JIRA_BASE_URL }}
          JIRA_USER_EMAIL: ${{ secrets.JIRA_USER_EMAIL }}
          JIRA_API_TOKEN: ${{ secrets.JIRA_API_TOKEN }}
```

### 4. Jira Dashboard Configuration
**Sprint Board Setup:**
- **Sprint Duration**: 2 weeks (aligned with PRD milestones)
- **Sprint Goals**: Linked to Week 1-2, 3-4, 5-6, 7-8 deliverables
- **Velocity Tracking**: Story points and cycle time metrics

**Epic Roadmap:**
```
Sprint 1-2: Data Infrastructure (PGPT-2) + Auth System (PGPT-3)
Sprint 3-4: Recommendation Engine (PGPT-1) + Model Pipeline (PGPT-5)  
Sprint 5-6: Dashboard & API Integration (PGPT-4)
Sprint 7-8: Testing, Optimization, Launch Prep
```

## Team Workflows

### Developer Workflow
1. **Story Assignment**: Pick story from sprint backlog
2. **Branch Creation**: Create branch `PGPT-XX-feature-description`
3. **Development**: Work on feature with regular commits
4. **Pull Request**: Create PR with Jira key in title
5. **Code Review**: Automatic Jira transition to "Code Review"
6. **Merge**: Automatic Jira transition to "Done"

### Data Engineer Workflow
1. **Data Pipeline Stories**: Use data-specific workflow
2. **Environment Tracking**: Tag issues with dev/intg/prod
3. **Data Quality Gates**: Link to acceptance criteria
4. **Performance Metrics**: Track pipeline SLAs in Jira

### Product Manager Workflow
1. **Epic Planning**: Create quarterly epics in Jira
2. **Sprint Planning**: Assign stories to sprints
3. **Stakeholder Updates**: Use Jira dashboards for status
4. **Metrics Tracking**: Monitor velocity and burn-down

## Reporting and Analytics

### Jira Reports
**Sprint Reports:**
- Burn-down charts
- Velocity tracking
- Scope creep analysis
- Team capacity planning

**Epic Progress:**
- Epic burn-down
- Feature completion rates
- Cross-team dependencies
- Release readiness

**Quality Metrics:**
- Bug introduction rate
- Cycle time analysis
- Code review efficiency
- Testing coverage

### Integration with GitHub Insights
**Combined Analytics:**
- Code review metrics (GitHub) + Story completion (Jira)
- Pull request velocity + Sprint velocity
- Issue resolution time + Development cycle time

## Security and Access Control

### Jira Permissions
**Project Roles:**
- **Project Administrator**: Product Manager, Tech Lead
- **Developer**: All engineering team members
- **Reporter**: QA, Business stakeholders
- **Viewer**: Management, external stakeholders

### GitHub Integration Security
**Required Secrets:**
```bash
JIRA_BASE_URL=https://yourcompany.atlassian.net
JIRA_USER_EMAIL=automation@yourcompany.com
JIRA_API_TOKEN=your-api-token
```

**Permission Scope:**
- Read access to GitHub repository
- Write access to Jira issues
- Webhook permissions for real-time updates

## Migration from GitHub Projects

### Data Migration Strategy
1. **Export GitHub Issues**: Use GitHub API to extract current issues
2. **Map to Jira Structure**: Convert labels to Jira issue types
3. **Preserve History**: Maintain comment history and timestamps
4. **Update References**: Link GitHub PRs to new Jira issues

### Transition Plan
**Week 1**: Parallel tracking (GitHub + Jira)
**Week 2**: Primary tracking in Jira, GitHub as backup
**Week 3**: Full Jira adoption, archive GitHub project

## Benefits of Jira Integration

### For Product Management
- **Advanced Roadmapping**: Epic planning and release management
- **Stakeholder Reporting**: Executive dashboards and status reports
- **Capacity Planning**: Team velocity and resource allocation
- **Risk Management**: Dependency tracking and bottleneck identification

### For Development Team
- **Familiar Workflows**: Smart commits and automated transitions
- **Better Planning**: Story estimation and sprint planning tools
- **Cross-team Visibility**: Dependencies and blocked work tracking
- **Quality Metrics**: Bug tracking and resolution analytics

### For Data Engineering
- **Pipeline Tracking**: Data quality gates and SLA monitoring
- **Environment Management**: dev/intg/prod workflow tracking
- **Performance Metrics**: Data pipeline SLA and quality tracking
- **Compliance**: Audit trails for data processing workflows

This Jira integration provides enterprise-grade project management while maintaining the developer experience and automation we've already built with GitHub.