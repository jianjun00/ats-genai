# GitHub Project Setup Guide

## Overview
This guide walks through setting up the complete GitHub project management system for Portfolio GPT MVP.

## Phase 1: Repository Setup

### 1. Issue Templates
✅ **Already Created**: Issue templates are in `.github/ISSUE_TEMPLATE/`
- `user_story.yml` - For feature development
- `epic.yml` - For organizing major features
- `bug_report.yml` - For defect tracking
- `tech_debt.yml` - For code quality improvements

### 2. GitHub Actions
✅ **Already Created**: Automation workflows in `.github/workflows/`
- `project_automation.yml` - Automated issue/PR management
- `sprint_reporting.yml` - Weekly sprint metrics

## Phase 2: GitHub Project Board Setup

### Step 1: Create New Project
1. Go to your GitHub repository
2. Click **Projects** tab
3. Click **New project**
4. Choose **Board** layout
5. Name: `Portfolio GPT MVP - 2 Month Delivery`

### Step 2: Configure Board Columns
Create these columns in order:

| Column Name | Purpose | Auto-add Rules |
|-------------|---------|----------------|
| 📋 **Backlog** | All new issues | Auto-add new issues |
| 🎯 **Sprint Ready** | Refined stories ready for development | Manual/label trigger |
| 🚧 **In Progress** | Active development work | Auto-add when PR opened |
| 👀 **Review** | Code review and testing | Auto-add when PR ready for review |
| ✅ **Done** | Completed and deployed | Auto-add when PR merged |
| 🚫 **Blocked** | Waiting on dependencies | Manual/label trigger |

### Step 3: Set Up Project Automation
1. In your project, go to **Settings**
2. Click **Manage access** and add team members
3. Set up **Workflows**:
   - **Auto-add items**: New issues and PRs
   - **Auto-archive items**: When issues/PRs are closed
   - **Set status**: Based on PR state changes

## Phase 3: Labels Configuration

### Step 1: Create Label Categories
Go to **Issues** → **Labels** → **New label**

#### Epic Labels
- `epic:recommendation-engine` (🎯 Blue) - Core forecasting system
- `epic:subscription-system` (🔐 Purple) - Authentication and tiers  
- `epic:dashboard` (📊 Green) - Frontend visualization
- `epic:model-pipeline` (🤖 Orange) - ML training and monitoring

#### Work Type Labels
- `type:feature` (✨ Blue) - New functionality
- `type:bug` (🐛 Red) - Defect fixes
- `type:epic` (🚀 Purple) - Major feature areas
- `type:tech-debt` (🔧 Yellow) - Code quality improvements
- `type:docs` (📚 Gray) - Documentation updates

#### Priority Labels
- `priority:critical` (🚨 Red) - Blocks MVP launch
- `priority:high` (⬆️ Orange) - Important for MVP
- `priority:medium` (➡️ Yellow) - Nice to have for MVP
- `priority:low` (⬇️ Green) - Post-MVP enhancement

#### Team Labels
- `team:backend` (💻 Blue) - Backend engineering work
- `team:frontend` (🎨 Green) - UI/UX development
- `team:data` (📊 Purple) - Data engineering tasks
- `team:ml` (🤖 Orange) - Model development
- `team:devops` (⚙️ Gray) - Infrastructure and deployment

#### Size Labels
- `size:XS` (🔹 Light Blue) - < 1 day
- `size:S` (🔸 Blue) - 1-2 days
- `size:M` (🔶 Orange) - 3-5 days
- `size:L` (🔴 Red) - 1 week
- `size:XL` (⚫ Black) - > 1 week, needs breakdown

## Phase 4: Milestones Setup

### Step 1: Create Sprint Milestones
Go to **Issues** → **Milestones** → **New milestone**

#### Sprint Milestones
1. **Sprint 1: Foundation (Weeks 1-2)**
   - Due date: [Start date + 2 weeks]
   - Description: Authentication system, database schema, basic model pipeline

2. **Sprint 2: Core Engine (Weeks 3-4)**
   - Due date: [Start date + 4 weeks]  
   - Description: Hourly forecast generation, recommendation logic, performance monitoring

3. **Sprint 3: Frontend & Integration (Weeks 5-6)**
   - Due date: [Start date + 6 weeks]
   - Description: Dashboard UI, API integration, chart visualization

4. **Sprint 4: Testing & Launch (Weeks 7-8)**
   - Due date: [Start date + 8 weeks]
   - Description: End-to-end testing, performance optimization, deployment

#### Release Milestones
- **MVP Beta**: Internal testing release
- **MVP v1.0**: Public launch

## Phase 5: Initial Epic Creation

### Create Epic Issues
Use the Epic issue template to create these initial epics:

1. **[EPIC]: Hourly Recommendation Engine**
   - Labels: `epic:recommendation-engine`, `type:epic`, `priority:critical`
   - Milestone: Sprint 2
   - Description: Core forecasting system with hourly price predictions

2. **[EPIC]: Subscription & Authentication System**
   - Labels: `epic:subscription-system`, `type:epic`, `priority:high`
   - Milestone: Sprint 1
   - Description: API authentication and tiered access control

3. **[EPIC]: Interactive Dashboard**
   - Labels: `epic:dashboard`, `type:epic`, `priority:high`
   - Milestone: Sprint 3
   - Description: Frontend visualization and user interface

4. **[EPIC]: Model Training Pipeline**
   - Labels: `epic:model-pipeline`, `type:epic`, `priority:medium`
   - Milestone: Sprint 2
   - Description: Automated model training and monitoring

## Phase 6: GitHub Actions Setup

### Step 1: Create Project Token
1. Go to **Settings** → **Developer settings** → **Personal access tokens**
2. Generate new token with `project` scope
3. Add as repository secret: `PROJECT_TOKEN`

### Step 2: Enable Workflows
1. Go to **Actions** tab
2. Enable GitHub Actions if not already enabled
3. Workflows will automatically trigger based on events

### Step 3: Test Automation
1. Create a test issue using the User Story template
2. Verify it automatically appears in project board
3. Create a PR and verify status updates

## Phase 7: Team Onboarding

### Step 1: Access Management
1. Add team members to repository with appropriate permissions:
   - **Admin**: Product Manager, Tech Lead
   - **Write**: All developers
   - **Read**: Stakeholders

2. Add team members to project board with **Write** access

### Step 2: Team Training
Conduct 30-minute session covering:
- How to create issues using templates
- Project board workflow
- Label usage conventions
- Sprint planning process

### Step 3: Working Agreements
Document team agreements:
- Daily standup via GitHub issue updates
- PR review requirements (2 reviewers for critical features)
- Definition of Ready and Done criteria
- Sprint commitment and change management

## Phase 8: Monitoring & Metrics

### Key Metrics to Track
1. **Velocity**: Story points completed per sprint
2. **Cycle Time**: Time from "In Progress" to "Done"
3. **Lead Time**: Time from "Backlog" to "Done"
4. **Burndown**: Progress toward sprint goals
5. **Quality**: Bug escape rate, review coverage

### Weekly Reviews
- **Monday**: Sprint planning using project board
- **Wednesday**: Mid-sprint check-in and blocker review
- **Friday**: Sprint review and retrospective
- **Automated**: Weekly sprint reports via GitHub Actions

## Phase 9: Continuous Improvement

### Regular Process Reviews
- **Weekly**: Retrospective and process adjustments
- **Monthly**: Label taxonomy and workflow optimization
- **Quarterly**: Tool effectiveness and automation improvements

### Success Criteria
- [ ] 100% of work tracked through GitHub issues
- [ ] 95% of issues properly labeled and sized
- [ ] 80% of PRs linked to issues
- [ ] Weekly sprint reports generated automatically
- [ ] Team velocity stable and predictable

## Troubleshooting

### Common Issues
1. **Issues not appearing in project**: Check auto-add rules and permissions
2. **Automation not working**: Verify GitHub Actions are enabled and tokens are valid
3. **Labels not applying**: Check label names match exactly in templates
4. **Team confusion**: Schedule additional training sessions

### Support Resources
- GitHub Projects documentation
- GitHub Actions workflow examples
- Team retrospective feedback
- Project metrics dashboard

This setup provides a comprehensive project management system that scales with team growth and project complexity while maintaining developer workflow efficiency.