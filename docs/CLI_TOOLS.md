# ATS Environment CLI Tools

This document describes the CLI tools for managing ATS environments.

## Overview

Two CLI tools are provided for environment-specific management:

- **`intg_cli`** - Manages the ats-intg (Integration) environment
- **`prod_cli`** - Manages the ats-prod (Production) environment with safety checks

## Installation

### Local Usage
```bash
# From project root
./scripts/intg_cli --help
./scripts/prod_cli --help
```

### Global Installation (Optional)
```bash
# Create symlinks for global access
sudo ln -s /home/jianjun/ats-genai/scripts/intg_cli /usr/local/bin/intg_cli
sudo ln -s /home/jianjun/ats-genai/scripts/prod_cli /usr/local/bin/prod_cli

# Then use from anywhere
intg_cli health
prod_cli health
```

## intg_cli - Integration Environment

### Database Operations
```bash
# Check database status
intg_cli db status

# Create manual backup
intg_cli db backup

# List available backups
intg_cli db list-backups

# Restore from backup
intg_cli db restore /path/to/backup.sql.gz
```

### Kubernetes Operations
```bash
# Check all Kubernetes resources
intg_cli k8s status

# Restart a deployment
intg_cli k8s restart postgres

# Get logs from a resource
intg_cli k8s logs postgres --lines 100
```

### File Storage Operations
```bash
# Check file storage statistics
intg_cli storage status

# Verify data integrity
intg_cli storage verify

# Verify specific symbol
intg_cli storage verify --symbol AAPL

# Query minute data
intg_cli storage query AAPL --days 7
```

### Health and Monitoring
```bash
# Comprehensive health check
intg_cli health

# Test Slack notifications
intg_cli test-slack
```

## prod_cli - Production Environment

### 🚨 Production Safety Features

The production CLI includes additional safety measures:

- **Confirmation prompts** for all destructive operations
- **Double confirmation** for database restore operations
- **Production warnings** in all output
- **Force flag** to skip confirmations (use with extreme caution)

### Database Operations
```bash
# Check database status (safe, no confirmation)
prod_cli db status

# Create manual backup (requires confirmation)
prod_cli db backup

# Create backup without confirmation (dangerous)
prod_cli db backup --force

# List available backups (safe)
prod_cli db list-backups

# Restore from backup (requires double confirmation)
prod_cli db restore /path/to/backup.sql.gz

# Force restore without confirmations (very dangerous)
prod_cli db restore backup.sql.gz --force
```

### Production Confirmation Flow

For dangerous operations, you'll see prompts like:
```
⚠️  PRODUCTION ENVIRONMENT WARNING
🔴 You are about to perform: Create PRODUCTION database backup
🏭 Environment: PROD
🚨 This action affects PRODUCTION data and services!

Type 'CONFIRM' to proceed (case-sensitive): CONFIRM
✅ Production action confirmed - proceeding...
```

For database restore, additional confirmation is required:
```
Type 'RESTORE-PRODUCTION' to continue: RESTORE-PRODUCTION
```

### Kubernetes Operations
```bash
# Check resources (safe)
prod_cli k8s status

# Restart deployment (requires confirmation)
prod_cli k8s restart postgres

# Force restart without confirmation
prod_cli k8s restart postgres --force

# Get logs (safe)
prod_cli k8s logs postgres --lines 50
```

### Health and Monitoring
```bash
# Health check with production-specific metrics
prod_cli health

# Test Slack notifications (requires confirmation)
prod_cli test-slack

# Force test notification
prod_cli test-slack --force
```

## Output Format

All commands return structured JSON output for programmatic use:

```json
{
  "status": "success|error|cancelled",
  "message": "Human readable message",
  "environment": "INTEGRATION|PRODUCTION",
  "timestamp": "2025-08-23T14:54:18.766375",
  "data": {...}
}
```

### Exit Codes
- `0` - Success
- `1` - Error
- `2` - Cancelled (production confirmations only)

## Common Usage Patterns

### Daily Health Check
```bash
# Check both environments
intg_cli health
prod_cli health
```

### Backup Management
```bash
# Create backups
intg_cli db backup
prod_cli db backup  # Will require confirmation

# List recent backups
intg_cli db list-backups | jq '.[] | select(.age_hours < 24)'
prod_cli db list-backups | jq '.[] | select(.age_hours < 24)'
```

### Troubleshooting
```bash
# Check system status
intg_cli k8s status
prod_cli k8s status

# Get logs for investigation
intg_cli k8s logs postgres --lines 1000
prod_cli k8s logs postgres --lines 1000

# Verify data integrity
intg_cli storage verify
prod_cli storage verify
```

### Emergency Operations
```bash
# Force operations in emergency (use sparingly)
prod_cli k8s restart postgres --force
prod_cli db backup --force

# Skip all confirmations (very dangerous)
prod_cli db restore emergency_backup.sql.gz --force
```

## Integration with Monitoring

### Scripted Health Checks
```bash
#!/bin/bash
# daily_health_check.sh

echo "=== Integration Environment ==="
intg_cli health | jq '.overall_healthy'

echo "=== Production Environment ==="
prod_cli health | jq '.overall_healthy'

# Alert if any environment is unhealthy
if ! intg_cli health | jq -e '.overall_healthy'; then
    echo "❌ Integration environment unhealthy"
    exit 1
fi

if ! prod_cli health | jq -e '.overall_healthy'; then
    echo "❌ Production environment unhealthy"
    exit 1
fi

echo "✅ All environments healthy"
```

### Automated Backups
```bash
#!/bin/bash
# automated_backup.sh

# Integration backup (no confirmation needed)
intg_cli db backup

# Production backup with force flag (for automation)
prod_cli db backup --force
```

## Security Considerations

1. **Production CLI Access**: Limit access to `prod_cli` to authorized personnel only
2. **Force Flag Usage**: The `--force` flag bypasses all safety checks - use only in emergencies
3. **Audit Logging**: All CLI operations are logged in respective environment logs
4. **Backup Security**: Backup files contain sensitive production data - secure appropriately
5. **Confirmation Requirements**: Never automate production operations without careful consideration

## Troubleshooting

### Common Issues

**Import Errors**:
```bash
# Ensure PYTHONPATH is set correctly
export PYTHONPATH="/home/jianjun/ats-genai/src:$PYTHONPATH"
```

**Kubernetes Access**:
```bash
# Ensure kubectl is configured for the correct cluster
kubectl config current-context
```

**Permission Errors**:
```bash
# Ensure scripts are executable
chmod +x scripts/intg_cli scripts/prod_cli
```

### Debug Mode
Add debug output by modifying the scripts to include verbose logging:

```bash
# Enable debug output
export DEBUG=1
intg_cli db status
```

## Development

### Adding New Commands
1. Add the command handler method to the respective CLI class
2. Update the argument parser in `create_parser()`
3. Add the command dispatch logic in `run()`
4. Test thoroughly in integration before deploying to production

### Testing
```bash
# Test integration CLI
python3 scripts/intg_cli.py health

# Test production CLI
python3 scripts/prod_cli.py health
```

## Support

For issues or feature requests:
1. Check this documentation
2. Review the CLI help: `intg_cli --help` or `prod_cli --help`
3. Examine the source code in `scripts/intg_cli.py` and `scripts/prod_cli.py`
4. Test commands in integration environment before using in production