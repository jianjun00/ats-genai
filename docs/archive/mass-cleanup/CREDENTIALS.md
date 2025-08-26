# Project Credentials and Access Tokens

## GitHub Tokens
- **Current Working Token**: `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` (jianjun00 account)
- **Expired Token**: `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` (no longer working)
- **Expired Token**: `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` (no longer working)

## Docker Hub
- **Username**: `dragonflyer762`
- **Token**: `dckr_pat_m7NlQdc5Rm6KGZnZZZ4iVlt9m20`

## Slack Integration
- **Webhook URL**: `https://hooks.slack.com/services/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`
- **Webhook Secret**: `slack-credentials` in ats-dev namespace
- **Backup Notifications**: `slack-webhook-secret` in all namespaces (ats-dev, ats-intg, ats-prod)
- **Channel**: Configured for ATS alerts across all environments

## Database Backup System
- **ats-dev**: Daily at 2:00 AM (backup + Slack notifications)
- **ats-intg**: Daily at 3:00 AM (backup + Slack notifications)  
- **ats-prod**: Daily at 1:00 AM (backup + critical Slack alerts)
- **Storage**: 20Ti each environment for comprehensive backup retention
- **Retention**: Custom + compressed SQL formats with integrity verification

## Usage Notes
- Use the current working GitHub token for API access, issue management, and workflow status
- GitHub token has full repository access including issues, PRs, and repository management
- Successfully used for creating GitHub Issues for 30-year price history project
- Docker Hub credentials for pushing/pulling images from dragonflyer762/ats-genai repository
- Slack webhooks configured in Kubernetes secrets for automated notifications
- Private repo - credentials documented for team access

## GitHub Token Capabilities (Verified 2025-08-25)
- ✅ Repository access: AkoloTechnologies/ats-genai
- ✅ Issue creation and management
- ✅ API access for repository operations
- ✅ Account: jianjun00 with MEMBER permissions