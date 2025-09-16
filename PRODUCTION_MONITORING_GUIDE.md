# ATS Data Quality Agent - Production Monitoring Guide

## 🎯 Overview

This guide provides comprehensive production monitoring and alerting for the ATS Data Quality Agent, including metrics collection, dashboard visualization, and automated alerts.

## 🚀 Quick Start

### 1. Start Monitoring Stack
```bash
# Start all monitoring services
./start_production_monitoring.sh
```

### 2. Access Points
- **Grafana Dashboard**: http://localhost:3000 (admin/ats_admin_2024)
- **Prometheus**: http://localhost:9090
- **AlertManager**: http://localhost:9093
- **Metrics Endpoint**: http://localhost:4000/metrics

## 📊 Metrics Overview

### Agent Status Metrics
- `ats_data_quality_agent_status` - Agent active status (0=inactive, 1=active)
- `ats_data_quality_agent_uptime_seconds` - Agent uptime
- `ats_data_quality_agent_restarts_total` - Number of agent restarts

### Issue Detection Metrics
- `ats_data_quality_issues_total` - Total number of issues detected
- `ats_data_quality_issues_by_severity_*` - Issues by severity level
- `ats_data_quality_symbols_affected` - Number of symbols with issues
- `ats_data_quality_overall_score` - Quality score (0-100)

### Performance Metrics
- `ats_data_quality_scan_duration_seconds` - Time for quality scans
- `ats_data_quality_api_request_duration_seconds` - API response time
- `ats_data_quality_agent_memory_usage_bytes` - Memory usage

### Vendor-Specific Metrics
- `ats_data_quality_issues_by_vendor_*` - Issues per vendor
- `ats_data_quality_vendor_availability_*` - Vendor data availability

## 🔔 Alert Rules

### Critical Alerts (Immediate Response)
| Alert Name | Condition | Response Time |
|------------|-----------|---------------|
| `DataQualityAgentDown` | Agent inactive > 2 minutes | Immediate |
| `HighCriticalDataQualityIssues` | Critical issues > 100 | 5 minutes |
| `DataQualityIssueSpike` | 1000+ new issues in 15 min | 2 minutes |
| `VendorDataFailure` | Vendor availability < 90% | 15 minutes |

### Warning Alerts (Monitor & Plan)
| Alert Name | Condition | Response Time |
|------------|-----------|---------------|
| `AgentNoIssueDetection` | No issues detected in 2h | 2 hours |
| `HighPriorityDataQualityIssues` | High priority > 15,000 | 15 minutes |
| `DataQualityScoreLow` | Quality score < 75% | 15 minutes |
| `AgentMemoryUsageHigh` | Memory > 2GB | 10 minutes |

## 📈 Dashboard Panels

### 1. Agent Overview
- Real-time agent status
- Total issues detected
- Quality score gauge
- Issues by severity

### 2. Performance Monitoring
- Issue detection rate
- Scan duration trends
- API response times
- Memory usage

### 3. Issue Analysis
- Issues by vendor
- Symbols affected
- Recent issue trends
- Alert history

### 4. System Health
- Agent uptime
- Database connections
- API error rates
- Memory usage

## 🛠️ Operations Guide

### Starting Services

```bash
# Start data quality agent
docker-compose -f docker-compose.intg.yml up -d analytics-intg

# Start monitoring stack
./start_production_monitoring.sh

# Verify all services
curl http://localhost:4000/health
curl http://localhost:4000/metrics
curl http://localhost:9090/-/healthy
curl http://localhost:3000/api/health
```

### Monitoring Commands

```bash
# View monitoring logs
docker-compose -f docker-compose.monitoring.yml logs -f

# Check Prometheus targets
curl http://localhost:9090/api/v1/targets

# Test alert rules
curl http://localhost:9090/api/v1/rules

# View active alerts
curl http://localhost:9093/api/v1/alerts
```

### Troubleshooting

#### Agent Not Reporting Metrics
1. Check agent service status: `docker ps | grep analytics-intg`
2. Verify metrics endpoint: `curl http://localhost:4000/metrics`
3. Check Prometheus scraping: http://localhost:9090/targets
4. Review agent logs: `docker logs ats-intg-analytics`

#### Alerts Not Firing
1. Verify alert rules: http://localhost:9090/rules
2. Check alert evaluation: http://localhost:9090/alerts
3. Test AlertManager: http://localhost:9093
4. Verify notification channels (Slack/Email)

#### Dashboard Not Loading
1. Check Grafana status: `curl http://localhost:3000/api/health`
2. Verify dashboard file: `grafana/ats-data-quality-agent-dashboard.json`
3. Check data sources: http://localhost:3000/datasources
4. Review Grafana logs: `docker logs ats-grafana`

## 🚨 Alert Response Procedures

### Critical: DataQualityAgentDown
1. **Immediate Action**: Check analytics service status
2. **Investigation**: Review service logs for crash/error
3. **Resolution**: Restart service or investigate root cause
4. **Follow-up**: Monitor for recurring issues

### Critical: HighCriticalDataQualityIssues
1. **Assessment**: Review issue types and affected symbols
2. **Priority**: Focus on critical trading symbols (AAPL, SPY, QQQ)
3. **Action**: Initiate data quality remediation processes
4. **Communication**: Notify trading and risk teams

### Warning: DataQualityScoreLow
1. **Analysis**: Identify degradation patterns
2. **Investigation**: Check vendor data quality
3. **Planning**: Schedule data quality improvement tasks
4. **Monitoring**: Increase monitoring frequency

## 📋 Maintenance Tasks

### Daily
- [ ] Check dashboard for active alerts
- [ ] Verify agent uptime and performance
- [ ] Review quality score trends
- [ ] Monitor issue detection patterns

### Weekly
- [ ] Review alert patterns and thresholds
- [ ] Analyze performance trends
- [ ] Update alert recipient lists
- [ ] Test notification channels

### Monthly
- [ ] Review and update alert rules
- [ ] Performance optimization review
- [ ] Capacity planning assessment
- [ ] Update monitoring documentation

## 🔧 Configuration Files

### Prometheus Configuration
- **File**: `grafana/prometheus.yml`
- **Purpose**: Metrics collection configuration
- **Key Settings**: Scrape intervals, alert rules, targets

### Alert Rules
- **File**: `grafana/ats_data_quality_agent_alerts.yml`
- **Purpose**: Define alert conditions and thresholds
- **Categories**: Agent health, issue detection, performance

### AlertManager Configuration
- **File**: `grafana/alertmanager.yml`
- **Purpose**: Alert routing and notification
- **Channels**: Slack, Email, Webhooks

### Grafana Dashboard
- **File**: `grafana/ats-data-quality-agent-dashboard.json`
- **Purpose**: Visual monitoring interface
- **Panels**: Status, metrics, trends, alerts

## 📞 Emergency Contacts

### On-Call Rotation
- **Primary**: Data Engineering Team Lead
- **Secondary**: Platform Engineering Team
- **Escalation**: VP of Engineering

### Communication Channels
- **Critical Alerts**: `#ats-critical-alerts` (Slack)
- **Data Quality**: `#ats-data-quality` (Slack)  
- **General Alerts**: `#ats-alerts` (Slack)

## 📚 Additional Resources

- [Prometheus Query Language (PromQL) Guide](https://prometheus.io/docs/prometheus/latest/querying/)
- [Grafana Dashboard Best Practices](https://grafana.com/docs/grafana/latest/best-practices/)
- [AlertManager Configuration](https://prometheus.io/docs/alerting/latest/configuration/)
- [ATS Data Quality Agent Documentation](./docs/UNIFIED_DATA_QUALITY_SERVICE_PRD_DRD.md)

---

**📞 Emergency Support**: For critical production issues, contact the on-call engineer via PagerDuty or escalate through the established on-call rotation.