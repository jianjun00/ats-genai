# Jira Project Structure for Portfolio GPT MVP

## Project Information
- **Project Name**: Portfolio GPT MVP
- **Project Key**: PGPT  
- **Project Type**: Software Development
- **Template**: Scrum

## Epic Structure

### PGPT-1: Data Pipeline Infrastructure 
**Priority**: High  
**Description**: Build robust data pipeline for 3000+ stocks with 5-year historical data backfill. Includes instrument population, daily price ingestion, and data reconciliation.

**Stories**:
- PGPT-10: Fix database migration system table prefixing
- PGPT-11: Complete dev_db database migrations  
- PGPT-12: Populate 3000 instruments in ats-dev environment
- PGPT-13: Implement 5-year daily price backfill
- PGPT-14: Data reconciliation and quality validation

### PGPT-2: Core Recommendation Engine
**Priority**: High  
**Description**: Develop ML-based recommendation engine with multi-modal transformer for hourly price forecasts and stock recommendations.

**Stories** (Future):
- PGPT-20: Universe state persistence implementation
- PGPT-21: Multi-modal transformer architecture
- PGPT-22: Recommendation scoring algorithm
- PGPT-23: Model training pipeline

### PGPT-3: Authentication & Subscription System  
**Priority**: Medium  
**Description**: Implement API key management, usage tracking, and tier-based access control (free vs premium).

**Stories** (Future):
- PGPT-30: API key management system
- PGPT-31: Usage tracking and rate limiting
- PGPT-32: Subscription tier management

### PGPT-4: Dashboard & API Integration
**Priority**: Medium  
**Description**: Build user-facing dashboard and REST API for accessing recommendations and portfolio management.

**Stories** (Future):
- PGPT-40: REST API development
- PGPT-41: React dashboard implementation
- PGPT-42: Real-time data integration

## Current Sprint (Sprint 1)

### Active Stories
1. **PGPT-10**: Fix database migration system table prefixing
   - **Status**: In Progress
   - **Assignee**: Data Engineer
   - **Story Points**: 2
   - **Description**: Migration 028 has incorrect {env}_ placeholder format. Update to use standard table names.
   - **Acceptance Criteria**: 
     - [ ] Replace {env}_ placeholders with standard table names
     - [ ] Migration 028 executes successfully
     - [ ] Database reaches version 28

2. **PGPT-11**: Complete dev_db database migrations
   - **Status**: To Do
   - **Assignee**: Data Engineer  
   - **Story Points**: 1
   - **Description**: Run remaining migrations (028, 029) to bring dev_db to latest schema version.
   - **Acceptance Criteria**:
     - [ ] Database migrations complete successfully
     - [ ] Database at version 29 (latest)
     - [ ] All tables have proper dev_ prefixing

3. **PGPT-12**: Populate 3000 instruments in ats-dev environment
   - **Status**: To Do
   - **Assignee**: Data Engineer
   - **Story Points**: 5  
   - **Description**: Scale instrument population from 50 to 3000 liquid stocks using port-forward approach.
   - **Acceptance Criteria**:
     - [ ] 3000 instruments populated in dev_instrument_polygon table
     - [ ] Data quality validation passes
     - [ ] Processing time under 30 minutes

4. **PGPT-13**: Implement 5-year daily price backfill
   - **Status**: To Do
   - **Assignee**: Data Engineer
   - **Story Points**: 8
   - **Description**: Backfill historical daily prices for 3000 stocks covering 2020-2025 period.
   - **Acceptance Criteria**:
     - [ ] Daily prices for 3000 stocks from 2020-01-01 to present
     - [ ] Data completeness > 95%
     - [ ] Multi-source reconciliation completed

5. **PGPT-14**: Data reconciliation and quality validation  
   - **Status**: To Do
   - **Assignee**: Data Engineer
   - **Story Points**: 3
   - **Description**: Validate data quality and generate completeness reports.
   - **Acceptance Criteria**:
     - [ ] Data quality report generated
     - [ ] Cross-source validation completed
     - [ ] Performance benchmarks established

## Sprint Planning

### Sprint 1 Goals (Current)
- **Duration**: 2 weeks
- **Goal**: Establish robust data foundation with 3000 stocks and 5-year history
- **Velocity Target**: 19 story points
- **Key Deliverables**:
  - Working dev_db with complete schema
  - 3000 instruments populated
  - 5-year price history backfilled
  - Data quality validated

### Sprint 2 Goals (Planned)  
- **Goal**: Begin recommendation engine development
- **Focus**: Universe state persistence and ML pipeline foundation

## GitHub Integration

### Branch Naming Convention
```
PGPT-{issue-number}-{short-description}
```

Examples:
- `PGPT-10-fix-migration-prefixing`
- `PGPT-12-populate-3000-instruments`
- `PGPT-13-price-backfill-pipeline`

### Commit Message Format
```
PGPT-{issue}: {brief description}

{detailed description}
- Bullet point changes
- More details

#time {hours}h #comment {progress notes}
```

### Pull Request Template
```markdown
## Related Jira Issue
PGPT-{issue-number}

## Summary
Brief description of changes

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass  
- [ ] Manual testing completed

## Deployment Notes
Any special deployment considerations
```

## Automation Rules

### GitHub → Jira Transitions
- **Branch Creation**: Auto-transition to "In Progress"
- **Pull Request Open**: Auto-transition to "In Review"  
- **Pull Request Merged**: Auto-transition to "Done"
- **Build Failure**: Auto-add comment with failure details

### Smart Commits
- Time tracking: `#time 2h 30m`
- Comments: `#comment Fixed migration issue`
- Transitions: `#in-progress` or `#done`

## Reporting

### Key Metrics
- **Sprint Velocity**: Story points completed per sprint
- **Cycle Time**: Days from "In Progress" to "Done"
- **Bug Rate**: Bugs per story point delivered
- **Code Review Time**: Hours in "In Review" status

### Dashboards
- **Sprint Dashboard**: Current sprint progress and burndown
- **Epic Dashboard**: Epic progress across sprints
- **Team Dashboard**: Individual velocity and workload
- **Quality Dashboard**: Bug rates and technical debt

This structure provides full traceability from high-level epics down to individual commits while maintaining the developer experience.