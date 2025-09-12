#!/usr/bin/env python3
"""
Create GitHub Issues for Universe Membership Logic Fixes
Creates comprehensive issues tracking the critical fixes needed
"""

import subprocess
import json
from datetime import datetime

def create_github_issues():
    """Create GitHub issues for universe membership fixes"""

    print("📋 CREATING GITHUB ISSUES FOR UNIVERSE MEMBERSHIP FIXES")
    print("="*70)

    issues = [
        {
            "title": "🚨 CRITICAL: Fix universe membership placeholder dates with actual qualification tracking",
            "body": """## Problem
95%+ of universe membership records have placeholder `start_at = '1995-01-01'` instead of actual volume qualification dates.

## Impact
- Historical analysis is meaningless with fake dates
- Cannot validate investment strategies or backtesting
- Universe timeline doesn't reflect real market dynamics

## Research Findings
Based on real market data analysis:
- **SMCI**: Should be `2023-01-09` (AI boom entry) not `1995-01-01`
- **MSTR**: Should be `2020-12-17` (Bitcoin strategy) not `1995-01-01`
- **PTON**: Should show exit `2023-04-27` when volume fell below threshold

## Solution
1. Replace placeholder dates with actual qualification analysis
2. Use 50-day rolling volume averages to find qualification dates
3. Set `start_at` when stock first exceeds $100M threshold
4. Set `end_at` when stock falls below $100M threshold

## Validation
- [x] Research completed with real market data
- [x] Test framework created
- [ ] Implementation needed

**Priority: P0 - Critical data integrity issue**
**Labels: bug, data-quality, universe-analytics**""",
            "labels": ["bug", "P0-critical", "data-quality", "universe-analytics"]
        },

        {
            "title": "⚡ Implement automated daily universe membership evaluation process",
            "body": """## Problem
No automated process exists to monitor when stocks qualify/disqualify for universe membership based on volume criteria.

## Current Issues
- Manual/bulk updates instead of criteria-based tracking
- No daily monitoring of $100M volume threshold
- Missing entry/exit events as they happen

## Solution
Implement `UniverseMembershipManager.evaluate_daily_membership()`:

```python
def evaluate_daily_membership(evaluation_date, universe_id):
    # 1. Calculate 50-day rolling volume averages
    current_qualifiers = get_current_qualifiers(evaluation_date)

    # 2. Get active universe members
    active_members = get_active_members(universe_id)

    # 3. Process exits (active members below threshold)
    for member in active_members:
        if member not in current_qualifiers:
            update_membership_exit(member, evaluation_date)

    # 4. Process entries (new qualifiers above threshold)
    for qualifier in current_qualifiers:
        if qualifier not in active_members:
            insert_new_membership(qualifier, evaluation_date)
```

## Implementation Files
- `src/domains/trading/services/universe_membership_manager.py` (created)
- Daily cron job/scheduler integration
- Logging and monitoring

**Priority: P0 - Critical business logic missing**
**Labels: enhancement, automation, universe-analytics**""",
            "labels": ["enhancement", "P0-critical", "automation", "universe-analytics"]
        },

        {
            "title": "📊 Implement historical data correction based on volume analysis",
            "body": """## Problem
Current universe membership data is fundamentally flawed with placeholder dates and missing historical tracking.

## Historical Analysis Required
Recalculate all start_at/end_at dates using actual volume data:

### Key Stocks Requiring Correction
| Stock | Current start_at | Correct start_at | Reason |
|-------|------------------|------------------|--------|
| SMCI | 1995-01-01 | 2023-01-09 | AI boom qualification |
| MSTR | 1995-01-01 | 2020-12-17 | Bitcoin strategy surge |
| PTON | 2019-09-26 | ~2020-04-15 | Pandemic volume surge |
| BYND | 2019-05-02 | ~2019-07-15 | Post-IPO hype qualification |

## Implementation Plan
1. **Volume Analysis Engine**: Calculate historical 50-day rolling averages
2. **Event Detection**: Find exact qualification/disqualification dates
3. **Membership Reconstruction**: Create correct membership records
4. **Multiple Period Support**: Handle re-entries (BYND volatility cycles)
5. **Validation**: Compare with research findings

## Success Criteria
- [ ] All placeholder dates removed
- [ ] Start dates match actual volume qualification events
- [ ] Multiple membership periods created for volatile stocks
- [ ] Historical timeline matches real market dynamics

**Priority: P1 - High impact data correction**
**Labels: data-migration, historical-analysis, universe-analytics**""",
            "labels": ["data-migration", "P1-high", "historical-analysis", "universe-analytics"]
        },

        {
            "title": "🔄 Support multiple membership periods for stocks with entry/exit cycles",
            "body": """## Problem
Current schema only supports one membership record per stock, missing volatility cycles and re-entries.

## Real Examples Requiring Multiple Periods
**BYND (Beyond Meat) Volatility Cycle:**
- Period 1: `2019-07-15` → `2022-07-11` (Hype cycle)
- Period 2: `2022-07-21` → `2022-07-22` (Brief requalification)
- Period 3: `2024-11-01` → Active (Hypothetical recovery)

**ARKB (Bitcoin ETF) Launch Volatility:**
- Exit: `2024-06-28` (below threshold)
- Re-entry: `2024-07-19` (volume recovery)

## Technical Implementation
### Database Schema (Already Supports)
```sql
-- Current schema allows multiple records per symbol
CREATE TABLE intg_universe_membership (
    universe_id INTEGER,
    symbol TEXT,
    start_at TIMESTAMP,
    end_at TIMESTAMP,  -- NULL = active
    instrument_id INTEGER
);
```

### Business Logic Updates
1. **Entry Processing**: Always create NEW record for requalification
2. **Exit Processing**: Set end_at on active record only
3. **Query Updates**: Handle multiple periods in API responses
4. **UI Updates**: Display multiple membership periods

## Validation Tests
- [x] Multiple entry/exit simulation test created
- [ ] Real volatility cycle testing
- [ ] UI display of multiple periods

**Priority: P1 - Critical for accurate tracking**
**Labels: enhancement, data-model, universe-analytics**""",
            "labels": ["enhancement", "P1-high", "data-model", "universe-analytics"]
        },

        {
            "title": "📅 Add scheduled daily job for universe membership monitoring",
            "body": """## Problem
No automated scheduling exists for daily universe membership evaluation.

## Requirements
### Daily Evaluation Job
- **Schedule**: Run daily at market close (6 PM ET)
- **Function**: `UniverseMembershipManager.evaluate_daily_membership()`
- **Scope**: All universes (currently ID 2, 3)
- **Logging**: Detailed entry/exit event logging

### Implementation Options
1. **Cron Job**: Traditional Unix cron scheduling
2. **K8s CronJob**: Kubernetes-native scheduling
3. **GitHub Actions**: Schedule workflow for consistency
4. **AWS Lambda**: Event-driven scheduling

### Monitoring Requirements
```python
# Example daily job output
{
    "evaluation_date": "2025-09-09",
    "universe_id": 2,
    "entries": [
        {"symbol": "NEW_STOCK", "volume": 150000000, "reason": "Exceeded threshold"}
    ],
    "exits": [
        {"symbol": "OLD_STOCK", "volume": 85000000, "reason": "Below threshold"}
    ],
    "total_qualifiers": 692,
    "total_active_after": 668
}
```

### Alert System
- Significant entry/exit events (>10 changes)
- Volume threshold boundary oscillations
- Data quality issues or processing failures

## Success Criteria
- [ ] Daily job scheduled and running
- [ ] Membership changes logged and monitored
- [ ] Alert system for significant events
- [ ] Historical tracking of daily evaluations

**Priority: P1 - Automation critical for ongoing accuracy**
**Labels: automation, monitoring, devops, universe-analytics**""",
            "labels": ["automation", "P1-high", "monitoring", "devops", "universe-analytics"]
        },

        {
            "title": "🔍 Add monitoring alerts for significant membership changes",
            "body": """## Problem
No visibility into universe membership changes or data quality issues.

## Monitoring Requirements
### Entry/Exit Event Alerts
- **High Volume Changes**: >10 entries or exits in single day
- **Large Stock Events**: Major stocks (AAPL, MSFT, etc.) entering/exiting
- **Threshold Oscillations**: Stocks rapidly entering/exiting (volatility)
- **Data Anomalies**: Unexpected volume patterns or missing data

### Alert Channels
1. **Slack Integration**: Real-time notifications to trading channel
2. **Email Alerts**: Summary reports for portfolio managers
3. **Dashboard Metrics**: Grafana/monitoring dashboard integration
4. **Audit Logs**: Permanent record of all membership changes

### Example Alert Messages
```
🚨 Universe Alert: Major Entry Event
• NVDA volume surge: $45.2B (was $28.1B)
• Entered universe 2 at 2025-09-09 18:00
• Reason: Earnings announcement volume spike

📊 Daily Universe Summary - Universe 2
• 3 entries: ABC (+$120M), DEF (+$105M), GHI (+$98M→$110M)
• 1 exit: XYZ ($95M, below threshold 5 days)
• Total active: 668 stocks
```

## Technical Implementation
### Monitoring Service
```python
class UniverseMonitoringService:
    def monitor_membership_changes(self, changes):
        # Analyze significance of changes
        # Generate appropriate alerts
        # Update monitoring dashboards
        pass
```

### Integration Points
- Universe membership manager
- Slack webhook API
- Email service
- Metrics collection (Prometheus/Grafana)

## Success Criteria
- [ ] Real-time alerts for significant events
- [ ] Daily summary reports generated
- [ ] Dashboard showing membership trends
- [ ] Audit trail of all changes

**Priority: P2 - Important for operational visibility**
**Labels: monitoring, alerts, observability, universe-analytics**""",
            "labels": ["monitoring", "P2-medium", "alerts", "observability", "universe-analytics"]
        }
    ]

    print(f"Creating {len(issues)} GitHub issues...\n")

    created_issues = []

    for i, issue in enumerate(issues, 1):
        print(f"📋 Creating Issue {i}: {issue['title'][:50]}...")

        # Create the issue using gh CLI
        try:
            # Prepare labels as comma-separated string
            labels_str = ",".join(issue['labels'])

            # Create issue with gh CLI
            result = subprocess.run([
                'gh', 'issue', 'create',
                '--title', issue['title'],
                '--body', issue['body'],
                '--label', labels_str
            ], capture_output=True, text=True)

            if result.returncode == 0:
                issue_url = result.stdout.strip()
                print(f"   ✅ Created: {issue_url}")
                created_issues.append({
                    'title': issue['title'],
                    'url': issue_url,
                    'labels': issue['labels']
                })
            else:
                print(f"   ❌ Failed: {result.stderr}")

        except Exception as e:
            print(f"   ❌ Error: {e}")

    print(f"\n📊 SUMMARY:")
    print(f"   ✅ Created {len(created_issues)} issues successfully")

    if created_issues:
        print(f"\n📋 CREATED ISSUES:")
        for issue in created_issues:
            print(f"   • {issue['title'][:60]}...")
            print(f"     {issue['url']}")

    return created_issues

if __name__ == "__main__":
    created_issues = create_github_issues()

    # Save issue list for reference
    with open('/home/jianjun/ats-genai-admin/universe_membership_issues.json', 'w') as f:
        json.dump(created_issues, f, indent=2)

    print(f"\n💾 Issue list saved to: universe_membership_issues.json")