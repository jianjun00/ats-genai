# ATS Data Quality Agent - Production Deployment

## 🚀 Quick Start (5 Minutes)

### One-Click Deployment
```bash
# Clone and deploy in one command
git clone <repository-url> && cd ats-genai-model
./scripts/deploy_production.sh
```

### Manual Deployment
```bash
# 1. Setup environment
python scripts/setup_environment.py production

# 2. Configure production settings
vim config/production.env

# 3. Start production services
./scripts/start_production.sh

# 4. Access dashboard
open http://localhost:4000/data-quality/dashboard
```

## 📋 Prerequisites

- Docker & Docker Compose
- PostgreSQL access
- API Keys: Polygon, Tiingo, EODHD
- SMTP server (for alerts)
- 5GB+ free disk space

## 🌐 Service Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    ATS Data Quality Agent                   │
├─────────────────────────────────────────────────────────────┤
│  📊 Dashboard (Port 4000)                                  │
│  ├── Real-time monitoring interface                        │
│  ├── Agent controls (Start/Stop/Config)                    │
│  ├── Issue management and resolution                       │
│  └── System health and performance metrics                 │
├─────────────────────────────────────────────────────────────┤
│  🤖 Data Quality Agent                                      │
│  ├── Autonomous monitoring (5-minute cycles)               │
│  ├── 12 specialized MCP tools                              │
│  ├── Intelligent issue resolution                          │
│  └── Enterprise alerting system                            │
├─────────────────────────────────────────────────────────────┤
│  🗄️ TimescaleDB Database (Port 5433)                      │
│  ├── Issue tracking and workflow management                │
│  ├── Performance metrics and alerting                      │
│  ├── Configuration storage                                 │
│  └── Historical data analysis                              │
└─────────────────────────────────────────────────────────────┘
```

## 🔧 Configuration

### Required Environment Variables
```bash
# Database
DB_PASSWORD=your_secure_password

# API Keys  
POLYGON_API_KEY=your_polygon_key
TIINGO_API_KEY=your_tiingo_key
EODHD_API_KEY=your_eodhd_key

# Email Alerts
EMAIL_SMTP_SERVER=smtp.company.com
EMAIL_RECIPIENTS=team@company.com

# Slack Alerts
SLACK_WEBHOOK_URL=https://hooks.slack.com/...
```

### Optional Customization
```bash
# Performance Tuning
AGENT_MONITORING_INTERVAL=300  # 5 minutes
AGENT_MAX_CONCURRENT_WORKFLOWS=20

# Alert Thresholds
CPU_CRITICAL_THRESHOLD=85
MEMORY_CRITICAL_THRESHOLD=90
```

## 🎯 Key Features

### 🔍 **Autonomous Data Quality Monitoring**
- **Real-time Scanning**: 5-minute cycles across all data sources
- **12 MCP Tools**: Specialized tools for assessment, resolution, validation
- **Smart Detection**: Missing data, extreme values, inconsistencies, staleness
- **Auto-Resolution**: 90%+ issues resolved without human intervention

### 📊 **Enterprise Dashboard**
- **Live Monitoring**: Real-time issue tracking and system health
- **Control Interface**: Start/stop agent, manage workflows, configure settings
- **Performance Metrics**: Quality scores, resolution rates, system utilization
- **Issue Management**: Manual resolution triggers and progress tracking

### 🚨 **Advanced Alerting**
- **Multi-Channel**: Email, Slack, webhook notifications
- **Intelligent Routing**: Severity-based escalation and rate limiting
- **Alert Correlation**: Prevents notification flooding
- **Acknowledgment System**: Track alert responses and resolution

### 📈 **Performance Optimization**
- **Resource Monitoring**: CPU, memory, disk usage tracking
- **Scalable Architecture**: Handle thousands of symbols and millions of records
- **Efficient Processing**: Batch operations and smart caching
- **Health Scoring**: 0-100 system and data quality scores

## 📚 Management Commands

### Daily Operations
```bash
# Check system status
./scripts/start_production.sh status

# View live logs
docker-compose -f docker-compose.production.yml logs -f

# Restart services
./scripts/start_production.sh restart

# Run health validation
python scripts/validate_system.py
```

### Agent Management
```bash
# Agent status
curl http://localhost:4000/agent/status

# Start/stop agent
curl -X POST http://localhost:4000/agent/start
curl -X POST http://localhost:4000/agent/stop

# View active workflows
curl http://localhost:4000/agent/workflows

# System health
curl http://localhost:4000/agent/system-health
```

### Configuration Management
```bash
# View current config
curl http://localhost:4000/agent/config

# Load production preset
curl -X POST http://localhost:4000/agent/config/preset/production

# Update monitoring interval
curl -X PUT http://localhost:4000/agent/config \
  -H "Content-Type: application/json" \
  -d '{"monitoring": {"cycle_interval_seconds": 600}}'
```

## 🏥 Health Monitoring

### System Health Indicators
- **Overall Health Score**: 0-100 composite score
- **Component Status**: Database, agent, MCP tools health
- **Resource Usage**: CPU, memory, disk utilization
- **Performance Metrics**: Response times, throughput rates

### Quality Metrics
- **Data Quality Score**: 0-100 data quality assessment
- **Issue Resolution Rate**: Percentage of auto-resolved issues
- **Coverage Statistics**: Symbols monitored, data freshness
- **Trend Analysis**: Quality improvement over time

## 🔒 Security & Backup

### Security Features
- **API Authentication**: Optional API key protection
- **Database Security**: Encrypted connections and user management
- **Network Isolation**: Docker network segmentation
- **Audit Logging**: Complete activity and access logging

### Backup Strategy
```bash
# Database backup
docker exec ats-prod-postgres pg_dump -U postgres prod_db > backup.sql

# Configuration backup
tar -czf config_backup.tar.gz config/

# Automated daily backups (add to crontab)
0 2 * * * /path/to/backup_script.sh
```

## 📞 Support & Troubleshooting

### Quick Diagnostics
```bash
# System health check
python scripts/quick_health_check.py

# API endpoint validation
python scripts/test_api_endpoints.py

# Database integration test
python scripts/test_database_integration.py
```

### Common Issues
| Issue | Solution |
|-------|----------|
| Agent won't start | Check database connection and configuration |
| High resource usage | Increase monitoring interval, reduce concurrent workflows |
| No issues detected | Verify data sources and detection thresholds |
| Dashboard not loading | Check service status and port accessibility |

### Log Locations
- **Agent logs**: `logs/agent/`
- **System logs**: `logs/system/`
- **Alert logs**: `logs/alerts/`
- **Docker logs**: `docker logs <container_name>`

## 📖 Documentation

- **[Production Guide](docs/PRODUCTION_DEPLOYMENT_GUIDE.md)**: Complete deployment and operations guide
- **[Operator Training](docs/OPERATOR_TRAINING_GUIDE.md)**: Comprehensive operator training manual
- **[API Reference](docs/API_REFERENCE.md)**: Complete API documentation
- **[Troubleshooting](docs/TROUBLESHOOTING_GUIDE.md)**: Common issues and solutions

## 🎯 Success Metrics

After successful deployment, you should see:
- ✅ All services healthy and running
- ✅ Dashboard accessible at http://localhost:4000/data-quality/dashboard
- ✅ Agent status "ACTIVE" with quality score >90
- ✅ System health score >85
- ✅ All API endpoints responding correctly
- ✅ Database connection established
- ✅ MCP tools operational

## 🚀 Ready for Production

The ATS Data Quality Agent is now **production-ready** with enterprise-grade:
- **Autonomous Operation**: 24/7 monitoring with minimal human intervention
- **Scalable Architecture**: Handle enterprise data volumes
- **Comprehensive Monitoring**: Real-time health and performance tracking  
- **Professional Support**: Complete documentation and troubleshooting guides
- **Operational Excellence**: Proven deployment and management procedures

**Start monitoring your data quality today!** 🎉