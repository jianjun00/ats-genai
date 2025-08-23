#!/usr/bin/env python3
"""
JIRA Issue Creation Script

This script helps create JIRA issues quickly from the command line
with proper templates and validation.

Usage:
    python scripts/create_jira_issue.py bug "Fix workflow errors"
    python scripts/create_jira_issue.py feature "Add dataset filtering"
    python scripts/create_jira_issue.py task "Update documentation"
    
Requirements:
    pip install jira python-dotenv
    
Environment Variables (.env file):
    JIRA_SERVER=https://your-company.atlassian.net
    JIRA_EMAIL=your-email@company.com
    JIRA_API_TOKEN=your-api-token
    JIRA_PROJECT=PGPT
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, Any, Optional

try:
    from jira import JIRA
    from dotenv import load_dotenv
except ImportError:
    print("❌ Missing required packages. Install with:")
    print("pip install jira python-dotenv")
    sys.exit(1)

# Load environment variables
load_dotenv()

# JIRA Configuration
JIRA_SERVER = os.getenv('JIRA_SERVER', 'https://your-company.atlassian.net')
JIRA_EMAIL = os.getenv('JIRA_EMAIL')
JIRA_API_TOKEN = os.getenv('JIRA_API_TOKEN')
JIRA_PROJECT = os.getenv('JIRA_PROJECT', 'PGPT')

# Issue templates
BUG_TEMPLATE = """## Problem Statement
**What is broken?**
- [Describe the specific functionality that is not working]
- [Include any error messages encountered]
- [Explain expected vs. actual behavior]

**How was this discovered?**
- [User report, testing, monitoring, etc.]
- [Environment where discovered (dev/staging/prod)]

**When did this start?**
- [Recent deployment? Specific commit? Always been broken?]

## Reproduction Steps
1. [Step-by-step instructions to reproduce the issue]
2. [Include specific inputs, configurations, or conditions]
3. [Note any environment-specific requirements]

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
"""

FEATURE_TEMPLATE = """## Business Justification
**Why is this needed?**
- [Business value or user benefit]
- [Problem this feature solves]
- [Strategic importance]

**Who requested this?**
- [Stakeholder name and role]
- [User persona or target audience]

## Feature Description
**What should the feature do?**
- [High-level functional description]
- [Key capabilities and behaviors]
- [Integration points with existing system]

**User Story:**
As a [user type], I want [functionality] so that [benefit/goal].

## Acceptance Criteria
**Functional Requirements:**
- [ ] [Specific feature behavior #1]
- [ ] [Specific feature behavior #2]
- [ ] [Error handling scenarios]
- [ ] [Data validation requirements]
- [ ] [Performance requirements]

**Non-Functional Requirements:**
- [ ] Performance benchmarks (response times, throughput)
- [ ] Security requirements
- [ ] Scalability considerations
- [ ] Accessibility standards
- [ ] Mobile responsiveness (if applicable)

## Technical Considerations
**Implementation Approach:**
- [Proposed technical solution]
- [Architecture changes needed]
- [Database schema modifications]
- [API changes required]

**Dependencies:**
- [Required infrastructure changes]
- [Third-party integrations]
- [Other features or tickets]

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
- [How will we measure feature success?]
- [Key performance indicators]
- [User adoption targets]
- [Business metrics impact]

## Related Issues
- Dependencies:
- Related features:
- Documentation tickets:
"""

TASK_TEMPLATE = """## Current State Problem
**What needs to be done?**
- [Describe the current situation or problem]
- [Why this task is needed]
- [Impact of not completing this task]

## Proposed Solution
**What needs to be changed?**
- [Specific work to be completed]
- [Approach or methodology]
- [Expected outcomes]

**Technical Approach:**
- [Implementation strategy]
- [Tools and technologies involved]
- [Testing or validation approach]

## Benefits
**Development Benefits:**
- [How this improves the development process]
- [Maintenance improvements]
- [Code quality improvements]

**System Benefits:**
- [Performance improvements]
- [Reliability improvements]
- [Security enhancements]

## Acceptance Criteria
- [ ] [Specific deliverable #1]
- [ ] [Specific deliverable #2]
- [ ] [Quality requirements met]
- [ ] [Documentation updated]
- [ ] [Testing completed]
- [ ] [No regression issues introduced]

## Effort Estimation
- Development effort: [hours/days]
- Testing effort: [hours/days]
- Documentation effort: [hours/days]
- Total estimated effort: [hours/days]

## Related Issues
- Dependencies:
- Related tasks:
- Follow-up work:
"""

def get_jira_client() -> Optional[JIRA]:
    """Initialize JIRA client with authentication."""
    if not all([JIRA_EMAIL, JIRA_API_TOKEN]):
        print("❌ Missing JIRA credentials. Please set environment variables:")
        print("   JIRA_EMAIL=your-email@company.com")
        print("   JIRA_API_TOKEN=your-api-token")
        print("   Get API token from: https://id.atlassian.com/manage-profile/security/api-tokens")
        return None
    
    try:
        jira = JIRA(
            server=JIRA_SERVER,
            basic_auth=(JIRA_EMAIL, JIRA_API_TOKEN)
        )
        # Test connection
        jira.current_user()
        return jira
    except Exception as e:
        print(f"❌ Failed to connect to JIRA: {e}")
        print("Check your credentials and server URL")
        return None

def get_issue_template(issue_type: str) -> str:
    """Get the appropriate template for the issue type."""
    templates = {
        'bug': BUG_TEMPLATE,
        'feature': FEATURE_TEMPLATE,
        'story': FEATURE_TEMPLATE,
        'task': TASK_TEMPLATE
    }
    return templates.get(issue_type.lower(), TASK_TEMPLATE)

def get_priority_from_user() -> str:
    """Get priority from user input."""
    priorities = {
        '1': 'Critical',
        '2': 'High', 
        '3': 'Medium',
        '4': 'Low'
    }
    
    print("\nSelect priority:")
    for key, value in priorities.items():
        print(f"  {key}. {value}")
    
    while True:
        choice = input("Enter priority (1-4): ").strip()
        if choice in priorities:
            return priorities[choice]
        print("Invalid choice. Please enter 1, 2, 3, or 4.")

def get_components_from_user() -> list:
    """Get components from user input."""
    components = [
        'Frontend',
        'Backend', 
        'Database',
        'CI/CD',
        'Infrastructure',
        'Documentation',
        'Security',
        'Performance'
    ]
    
    print("\nSelect components (comma-separated numbers):")
    for i, component in enumerate(components, 1):
        print(f"  {i}. {component}")
    
    while True:
        choices = input("Enter component numbers (e.g., 1,3,5): ").strip()
        if not choices:
            return []
        
        try:
            indices = [int(x.strip()) for x in choices.split(',')]
            selected = [components[i-1] for i in indices if 1 <= i <= len(components)]
            if selected:
                return selected
        except (ValueError, IndexError):
            pass
        
        print("Invalid input. Please enter comma-separated numbers from the list.")

def create_jira_issue(issue_type: str, summary: str, interactive: bool = False) -> Optional[str]:
    """Create a JIRA issue with the specified type and summary."""
    
    # Validate issue type
    valid_types = ['bug', 'feature', 'story', 'task']
    if issue_type.lower() not in valid_types:
        print(f"❌ Invalid issue type. Must be one of: {', '.join(valid_types)}")
        return None
    
    # Get JIRA client
    jira = get_jira_client()
    if not jira:
        return None
    
    # Get template
    description = get_issue_template(issue_type)
    
    # Interactive mode for additional details
    if interactive:
        print(f"\n📝 Creating {issue_type.upper()} issue: {summary}")
        priority = get_priority_from_user()
        components = get_components_from_user()
        
        # Get labels
        print(f"\nSuggested labels for {issue_type}:")
        suggested_labels = {
            'bug': ['bug', 'defect'],
            'feature': ['feature', 'enhancement'], 
            'story': ['feature', 'enhancement'],
            'task': ['task', 'technical-debt']
        }.get(issue_type.lower(), ['task'])
        
        labels_input = input(f"Labels (default: {','.join(suggested_labels)}): ").strip()
        labels = labels_input.split(',') if labels_input else suggested_labels
        labels = [label.strip() for label in labels]
        
    else:
        # Default values for non-interactive mode
        priority = 'Medium'
        components = []
        labels = {
            'bug': ['bug'],
            'feature': ['feature'], 
            'story': ['feature'],
            'task': ['task']
        }.get(issue_type.lower(), ['task'])
    
    # Map issue type for JIRA
    jira_issue_type = {
        'bug': 'Bug',
        'feature': 'Story',
        'story': 'Story', 
        'task': 'Task'
    }.get(issue_type.lower(), 'Task')
    
    # Create issue
    issue_dict = {
        'project': {'key': JIRA_PROJECT},
        'summary': summary,
        'description': description,
        'issuetype': {'name': jira_issue_type},
        'priority': {'name': priority},
        'labels': labels
    }
    
    # Add components if specified
    if components:
        issue_dict['components'] = [{'name': comp} for comp in components]
    
    try:
        issue = jira.create_issue(fields=issue_dict)
        print(f"✅ Created JIRA issue: {issue.key}")
        print(f"📋 URL: {JIRA_SERVER}/browse/{issue.key}")
        print(f"📝 Title: {summary}")
        print(f"🏷️  Priority: {priority}")
        if components:
            print(f"🔧 Components: {', '.join(components)}")
        print(f"🏷️  Labels: {', '.join(labels)}")
        
        # Suggest next steps
        print(f"\n🔄 Next Steps:")
        print(f"1. Edit the issue description with specific details")
        print(f"2. Create development branch:")
        print(f"   git checkout -b {issue.key}/{'fix' if issue_type == 'bug' else 'feature'}-{summary.lower().replace(' ', '-')[:20]}")
        
        return issue.key
        
    except Exception as e:
        print(f"❌ Failed to create JIRA issue: {e}")
        return None

def main():
    """Main function to handle command line arguments."""
    if len(sys.argv) < 3:
        print("Usage: python create_jira_issue.py <type> <summary> [--interactive]")
        print("\nIssue Types:")
        print("  bug      - Something is broken or not working")
        print("  feature  - New functionality or enhancement") 
        print("  story    - User story (same as feature)")
        print("  task     - General work item, technical debt, docs")
        print("\nExamples:")
        print("  python create_jira_issue.py bug 'Fix workflow dependency errors'")
        print("  python create_jira_issue.py feature 'Add dataset filtering functionality'")
        print("  python create_jira_issue.py task 'Update deployment documentation'")
        print("  python create_jira_issue.py bug 'Fix login errors' --interactive")
        sys.exit(1)
    
    issue_type = sys.argv[1]
    summary = sys.argv[2]
    interactive = '--interactive' in sys.argv or '-i' in sys.argv
    
    # Create the issue
    issue_key = create_jira_issue(issue_type, summary, interactive)
    
    if issue_key:
        print(f"\n🎉 JIRA issue {issue_key} created successfully!")
    else:
        print("❌ Failed to create JIRA issue")
        sys.exit(1)

if __name__ == "__main__":
    main()