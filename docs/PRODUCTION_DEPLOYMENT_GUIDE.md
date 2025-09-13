# ATS Data Quality Agent - Production Deployment Guide

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose installed
- PostgreSQL database (TimescaleDB recommended)
- Network access to data sources (Polygon, Tiingo, EODHD APIs)
- SMTP server for email alerts (optional)
- Slack workspace for alerts (optional)

### 1. Initial Setup

```bash
# Clone the repository
git clone <repository-url>
cd ats-genai-model

# Run environment setup
python scripts/setup_environment.py production

# Edit configuration with your values
vim config/production.env
vim config/agent_config.json
```

### 2. Configure Environment

Edit `config/production.env`:

```bash
# Database Configuration
DB_HOST=your-postgres-host
DB_PASSWORD=your-secure-password

# API Keys
POLYGON_API_KEY=your-polygon-key
TIINGO_API_KEY=your-tiingo-key
EODHD_API_KEY=your-eodhd-key

# Email Alerts
EMAIL_SMTP_SERVER=smtp.yourcompany.com
EMAIL_RECIPIENTS=team@yourcompany.com

# Slack Alerts
SLACK_WEBHOOK_URL=your-slack-webhook-url
```

### 3. Deploy Services

```bash
# Start the system
./scripts/start_production.sh

# Verify deployment
python scripts/quick_health_check.py
python scripts/validate_system.py
```

### 4. Access Dashboard

Open: `http://your-server:4000/data-quality/dashboard`

## 📊 System Architecture

### Components

1. **Data Quality Agent** - Autonomous monitoring and issue resolution
2. **MCP Tools** - 12 specialized tools for data quality management
3. **System Monitor** - Real-time resource and health monitoring
4. **Alert Manager** - Multi-channel notifications and escalation
5. **Dashboard** - Real-time control and monitoring interface

### Data Flow

```
Data Sources (APIs) → Database → Quality Scanning → Issue Detection → 
Agent Decision → MCP Tool Execution → Resolution → Monitoring
```

## ⚙️ Configuration Management

### Agent Configuration

Key configuration sections in `config/agent_config.json`:

```json
{
  "monitoring": {
    "cycle_interval_seconds": 300,
    "max_concurrent_workflows": 20
  },
  "issue_thresholds": {
    "quality_score_critical_threshold": 50,
    "extreme_volume_multiplier": 50.0
  },
  "notifications": {
    "enable_email_notifications": true,
    "enable_slack_notifications": true
  }
}
```

### Environment-Specific Settings

- **Development**: Faster cycles, verbose logging, local notifications
- **Production**: Stable cycles, optimized logging, enterprise notifications

### Runtime Configuration

Update configuration via API:

```bash
curl -X PUT http://localhost:4000/agent/config \
  -H "Content-Type: application/json" \
  -d '{"monitoring": {"cycle_interval_seconds": 600}}'
```

## 🔧 Operations Guide

### Starting/Stopping Services

```bash
# Start all services
docker-compose -f docker-compose.production.yml up -d

# Stop all services
docker-compose -f docker-compose.production.yml down

# Restart specific service
docker-compose -f docker-compose.production.yml restart ats-prod-analytics
```

### Monitoring Agent Status

```bash
# Quick health check
curl http://localhost:4000/agent/status

# Detailed health information
curl http://localhost:4000/agent/system-health

# View active alerts
curl http://localhost:4000/agent/alerts
```

### Log Management

```bash
# View agent logs
docker logs ats-prod-analytics --tail 50

# View system logs
tail -f logs/system/system_health.log

# View alert logs
tail -f logs/alerts/alerts_production.jsonl
```

## 🚨 Alert Management

### Alert Types

1. **System Alerts**: CPU, Memory, Disk usage
2. **Agent Alerts**: Monitoring failures, workflow issues
3. **Data Quality Alerts**: Missing data, extreme values
4. **Database Alerts**: Connection failures, query errors

### Notification Channels

- **Email**: SMTP-based notifications
- **Slack**: Webhook-based channel notifications
- **Webhooks**: Custom integrations

### Alert Workflow

```
Issue Detection → Alert Creation → Notification → 
Acknowledgment → Investigation → Resolution
```

## 📈 Performance Tuning

### Resource Optimization

Monitor these key metrics:

- **CPU Usage**: Keep below 70% average
- **Memory Usage**: Keep below 80% of available
- **Disk Space**: Keep below 85% usage
- **Database Connections**: Monitor active connections

### Configuration Tuning

Adjust based on your environment:

```json
{
  "monitoring": {
    "cycle_interval_seconds": 300,  // Increase for lower resource usage
    "max_concurrent_workflows": 20  // Adjust based on system capacity
  }
}
```

### Database Optimization

- Use connection pooling
- Optimize query performance
- Regular maintenance (VACUUM, ANALYZE)
- Monitor table sizes and growth

## 🔒 Security Considerations

### Database Security

- Use strong passwords
- Limit database connections
- Enable SSL connections
- Regular security updates

### API Security

- Secure API key storage
- Network access controls
- Rate limiting
- Audit logging

### System Security

- Regular OS updates
- Firewall configuration
- Container security scanning
- Access control

## 🛠️ Troubleshooting

### Common Issues

#### Agent Not Starting

```bash
# Check logs
docker logs ats-prod-analytics

# Check configuration
python -c "from src.agents.agent_config import get_config_manager; print(get_config_manager().get_config())"

# Test database connection
python scripts/test_database_integration.py
```

#### Database Connection Issues

```bash
# Test database connectivity
docker exec ats-prod-postgres pg_isready -U postgres

# Check network connectivity
docker network ls
docker inspect ats-network
```

#### High Resource Usage

```bash
# Check system resources
docker stats

# Review agent configuration
curl http://localhost:4000/agent/config

# Check active workflows
curl http://localhost:4000/agent/workflows
```

### Performance Issues

#### Slow Dashboard Loading

- Check database query performance
- Review log file sizes
- Monitor network latency

#### Memory Leaks

- Monitor container memory usage
- Review log retention settings
- Check for hanging processes

## 📊 Monitoring & Metrics

### Key Performance Indicators

1. **System Health Score**: Overall system health (0-100)
2. **Data Quality Score**: Quality of monitored data (0-100)
3. **Agent Uptime**: Percentage of time agent is operational
4. **Issue Resolution Rate**: Percentage of issues automatically resolved
5. **Alert Response Time**: Time from issue detection to notification

### Monitoring Tools Integration

#### Prometheus/Grafana

```yaml
# Example metrics endpoint
- job_name: 'ats-agent'
  static_configs:
    - targets: ['localhost:4000']
  metrics_path: '/agent/metrics'
```

#### ELK Stack

```json
{
  "log_format": "structured",
  "log_path": "logs/agent/*.jsonl",
  "index_pattern": "ats-agent-*"
}
```

## 🔄 Backup & Recovery

### Database Backup

```bash
# Automated backup (daily)
docker exec ats-prod-postgres pg_dump -U postgres prod_db > backup/prod_db_$(date +%Y%m%d).sql

# Restore from backup
docker exec -i ats-prod-postgres psql -U postgres prod_db < backup/prod_db_20241201.sql
```

### Configuration Backup

```bash
# Backup configuration
tar -czf backup/config_backup_$(date +%Y%m%d).tar.gz config/

# Backup logs (optional)
tar -czf backup/logs_backup_$(date +%Y%m%d).tar.gz logs/
```

### Disaster Recovery

1. **Data Recovery**: Restore from database backups
2. **Configuration Recovery**: Restore configuration files
3. **Service Recovery**: Redeploy containers with restored config
4. **Validation**: Run full system validation

## 📞 Support & Maintenance

### Regular Maintenance Tasks

- **Daily**: Check alert summary, review system health
- **Weekly**: Review performance metrics, update configurations if needed
- **Monthly**: System validation, log cleanup, security updates

### Support Contacts

- **Technical Issues**: Review logs and documentation
- **Configuration Help**: Refer to configuration examples
- **Performance Issues**: Use monitoring and troubleshooting guides

### Version Updates

```bash
# Update to new version
docker pull dragonflyer762/ats-genai:latest
docker-compose -f docker-compose.production.yml down
docker-compose -f docker-compose.production.yml up -d

# Validate update
python scripts/validate_system.py
```

## 📚 Additional Resources

- **API Documentation**: See REST API endpoints list
- **Configuration Reference**: Complete configuration options
- **Architecture Guide**: Detailed system architecture
- **Troubleshooting Guide**: Common issues and solutions