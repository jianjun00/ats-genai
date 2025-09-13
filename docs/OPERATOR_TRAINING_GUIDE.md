# ATS Data Quality Agent - Operator Training Guide

## 👨‍💻 Overview

This guide trains operations team members on managing the ATS Data Quality Agent system. It covers daily operations, monitoring, troubleshooting, and emergency procedures.

## 🎯 Learning Objectives

After completing this training, operators will be able to:

1. Monitor system health and performance
2. Manage alerts and notifications
3. Perform basic troubleshooting
4. Handle configuration changes
5. Respond to emergencies

## 📊 Dashboard Navigation

### Accessing the Dashboard

URL: `http://your-server:4000/data-quality/dashboard`

### Dashboard Components

#### 1. Agent Status Section
Located in the header, shows:
- **Agent Status**: ACTIVE/STOPPED/ERROR
- **Active Workflows**: Number of running workflows
- **Issues**: Number of pending issues

#### 2. Control Buttons
- **▶️ Start**: Start agent monitoring
- **⏹️ Stop**: Stop agent monitoring  
- **📋 Workflows**: View active workflows
- **📊 Metrics**: Performance metrics
- **⚙️ Config**: Configuration management
- **🩺 Health**: System health status
- **🚨 Alerts**: Alert management

#### 3. Statistics Cards
- **Total Issues**: Current data quality issues
- **Critical Issues**: High-priority issues requiring attention
- **High Priority**: Important issues
- **Symbols Affected**: Number of affected data symbols
- **Quality Score**: Overall data quality (0-100)

#### 4. Issues List
Shows detected data quality issues with:
- Issue description and severity
- Affected symbol and date
- Issue type and vendor source
- Action buttons for manual intervention

## 🔧 Daily Operations

### Morning Checklist

1. **Access Dashboard**
   ```
   Open: http://your-server:4000/data-quality/dashboard
   ```

2. **Check Agent Status**
   - Verify agent shows "ACTIVE"
   - Check for any error messages
   - Note number of active workflows

3. **Review Quality Score**
   - Target: >90 (Excellent)
   - >75 (Good) - monitor closely
   - <75 (Poor) - investigate issues

4. **Check Active Alerts**
   ```
   Click: 🚨 Alerts button
   Review: Any critical or high-priority alerts
   Action: Acknowledge/resolve as needed
   ```

5. **Review System Health**
   ```
   Click: 🩺 Health button
   Check: CPU, Memory, Disk usage
   Verify: All metrics in green ranges
   ```

### Ongoing Monitoring

#### Every 2 Hours
- Check dashboard for new issues
- Review alert notifications (email/Slack)
- Monitor system performance

#### Issue Response Workflow

1. **Issue Detection**
   - Dashboard shows new issues
   - Email/Slack notification received
   - Quality score decreases

2. **Issue Assessment**
   ```
   Review issue details:
   - Severity (Critical/High/Medium/Low)
   - Affected symbol/date
   - Issue type (missing_data, extreme_volume, etc.)
   ```

3. **Action Decision**
   - **Automatic**: Let agent handle
   - **Manual**: Use action buttons
   - **Escalate**: Contact technical team

4. **Manual Actions Available**
   - **🔄 Trigger Backfill**: For missing data
   - **🔍 Cross-Validate**: For suspicious values
   - **🔄 Auto-Deduplicate**: For duplicate records
   - **🕵️ Investigate**: For unknown issues

### Evening Checklist

1. **Review Daily Summary**
   - Total issues processed
   - Resolution success rate
   - Any unresolved critical issues

2. **Check System Health**
   - Resource usage trends
   - Active alerts status
   - Performance metrics

3. **Prepare Handover Notes**
   - Outstanding issues
   - Actions taken
   - Items requiring follow-up

## 🚨 Alert Management

### Alert Severities

#### Critical (Red)
- System failures
- Agent stopped unexpectedly  
- Database connection failures
- Multiple consecutive data quality failures

**Response**: Immediate action required (within 15 minutes)

#### High (Orange)  
- High CPU/memory usage
- Significant data quality degradation
- Multiple workflow failures

**Response**: Action required within 1 hour

#### Medium (Yellow)
- Moderate resource usage
- Minor data quality issues
- Individual workflow failures

**Response**: Action required within 4 hours

#### Low (Blue)
- Informational messages
- Successful completions
- Minor performance variations

**Response**: No immediate action required

### Alert Actions

#### Acknowledging Alerts
```
1. Click: 🚨 Alerts button
2. Find the alert
3. Click: ✓ Ack button
4. Alert marked as acknowledged
```

#### Resolving Alerts
```
1. After fixing the underlying issue
2. Click: ✓ Resolve button
3. Alert removed from active list
```

### Notification Channels

#### Email Notifications
- Sent to configured email addresses
- Include alert details and resolution links
- Check email for alerts when away from dashboard

#### Slack Notifications
- Posted to configured Slack channel
- Include severity color coding
- Use for team coordination

## 🛠️ Troubleshooting Guide

### Common Issues & Solutions

#### Agent Status Shows "STOPPED"

**Symptoms**: Agent shows stopped, no new issue detection

**Steps**:
1. Click **▶️ Start** button
2. Wait 30 seconds for startup
3. Check for error messages
4. If fails to start, contact technical team

#### High CPU/Memory Usage

**Symptoms**: System health shows red warnings

**Steps**:
1. Check **🩺 Health** for current usage
2. Review active workflows in **📋 Workflows**
3. If usage >90%, consider:
   - Reducing monitoring frequency
   - Stopping non-critical workflows
   - Restarting agent if necessary

#### Dashboard Not Loading

**Symptoms**: Cannot access dashboard URL

**Steps**:
1. Check network connectivity
2. Verify server is running: `ping your-server`
3. Check if service is up: `curl http://your-server:4000/health`
4. If service down, contact technical team

#### No New Data Issues Detected

**Symptoms**: No issues shown despite expecting problems

**Steps**:
1. Verify agent is **ACTIVE**
2. Check last scan time in agent status
3. Review configuration for monitoring frequency
4. Test with **🔍 Cross-Validate** action on known symbol

### Emergency Procedures

#### Critical System Failure

**Definition**: Agent completely non-responsive, multiple critical alerts

**Immediate Actions**:
1. Document current status (screenshot dashboard)
2. Check system health metrics
3. Note any recent changes or updates
4. Contact technical team immediately
5. Prepare for potential system restart

#### Data Quality Crisis

**Definition**: Quality score <30, multiple critical data issues

**Immediate Actions**:
1. Acknowledge all critical alerts
2. Review affected symbols and dates
3. Use manual action buttons to trigger immediate fixes
4. Escalate to data team and technical team
5. Prepare incident report

#### Database Connection Loss

**Definition**: Agent cannot connect to database

**Immediate Actions**:
1. Note exact time of failure
2. Check if database server is accessible
3. Review recent database activities
4. Contact DBA team and technical team
5. Monitor for service restoration

## ⚙️ Configuration Management

### Basic Configuration Changes

#### Adjusting Monitoring Frequency

```
1. Click: ⚙️ Config button
2. Find: "Cycle Interval (seconds)"
3. Adjust: 300 (5 min) to 600 (10 min) to reduce load
4. Click: 💾 Save Configuration
```

#### Updating Alert Thresholds

```
1. Click: ⚙️ Config button
2. Find: "Issue Thresholds" section
3. Adjust thresholds as needed
4. Click: 💾 Save Configuration
```

#### Environment Switching

```
For development testing:
1. Click: ⚙️ Config button
2. Click: 🔧 Dev Mode button
3. Confirm the change
```

### When NOT to Change Configuration

- During high-load periods
- When critical alerts are active
- Without coordination with technical team
- During data processing windows

## 📊 Performance Monitoring

### Key Metrics to Monitor

#### System Performance
- **CPU Usage**: Target <70%
- **Memory Usage**: Target <80%
- **Disk Usage**: Target <85%
- **Network**: Monitor for bottlenecks

#### Agent Performance  
- **Workflow Success Rate**: Target >95%
- **Average Resolution Time**: Monitor trends
- **Issues Processed Per Hour**: Track productivity
- **Health Score**: Target >90

#### Data Quality Metrics
- **Overall Quality Score**: Target >90
- **Critical Issues**: Target 0
- **High Priority Issues**: Target <5
- **Symbols Affected**: Monitor for increases

### Performance Trends

#### Daily Patterns
- Higher activity during market hours
- Lower activity during weekends
- Batch processing during overnight hours

#### Weekly Patterns
- Monday: Higher issue volume (weekend data)
- Friday: Lower activity (market close)
- Sunday: Maintenance windows

#### Monthly Patterns
- Month-end: Higher data volume
- Quarter-end: Increased validation
- Holiday periods: Reduced activity

## 📋 Reporting

### Daily Reports

#### Morning Report
```
Date: [Date]
Agent Status: [Active/Stopped/Error]
Quality Score: [Score]/100
Critical Issues: [Count]
High Priority Issues: [Count]
Overnight Issues: [Summary]
```

#### Evening Report
```
Date: [Date]
Issues Processed: [Count]
Resolution Rate: [Percentage]
Manual Interventions: [Count]
Outstanding Issues: [List]
Performance Notes: [Comments]
```

### Weekly Reports

#### Performance Summary
- Average quality score
- Total issues processed
- Resolution success rate
- System uptime percentage
- Notable incidents

#### Trend Analysis
- Quality score trends
- Issue type frequency
- Performance metrics
- Resource usage trends

### Incident Reports

#### Required Information
- Date and time of incident
- Symptoms observed
- Actions taken
- Resolution method
- Duration of impact
- Root cause (if known)
- Prevention measures

## 🎓 Training Exercises

### Exercise 1: Basic Navigation
1. Access the dashboard
2. Identify current agent status
3. Review quality score and issues
4. Navigate through all control buttons

### Exercise 2: Alert Management
1. Create a test alert (if possible)
2. Practice acknowledging alerts
3. Practice resolving alerts
4. Check notification channels

### Exercise 3: Troubleshooting Simulation
1. Simulate agent stop scenario
2. Practice restart procedure
3. Monitor startup process
4. Verify successful restart

### Exercise 4: Configuration Changes
1. Access configuration panel
2. Make a minor configuration change
3. Save and verify change
4. Restore original configuration

### Exercise 5: Performance Monitoring
1. Check current system health
2. Review performance metrics
3. Identify any concerning trends
4. Document observations

## 📞 Escalation Procedures

### Level 1: Operations Team
**Handle**: Routine monitoring, basic alerts, standard procedures

### Level 2: Technical Team
**Escalate for**:
- Agent fails to start after restart
- Database connectivity issues
- Configuration problems
- Performance degradation

### Level 3: Engineering Team
**Escalate for**:
- System architecture issues
- Code defects
- Major performance problems
- Security incidents

### Emergency Contacts
- **Technical Team Lead**: [Contact Info]
- **Database Administrator**: [Contact Info]  
- **On-Call Engineer**: [Contact Info]
- **Manager**: [Contact Info]

## ✅ Certification Checklist

### Basic Operations ✓
- [ ] Can access and navigate dashboard
- [ ] Can identify agent status and health
- [ ] Can interpret quality scores and issues
- [ ] Can acknowledge and resolve alerts

### Monitoring ✓
- [ ] Can perform daily health checks
- [ ] Can identify performance issues
- [ ] Can monitor system resources
- [ ] Can track quality trends

### Troubleshooting ✓
- [ ] Can restart agent services
- [ ] Can identify common issues
- [ ] Can execute basic troubleshooting steps
- [ ] Can escalate appropriately

### Configuration ✓
- [ ] Can access configuration panel
- [ ] Can make basic configuration changes
- [ ] Can switch between environments
- [ ] Knows when NOT to change configuration

### Emergency Response ✓
- [ ] Can identify emergency situations
- [ ] Can execute emergency procedures
- [ ] Can contact appropriate support
- [ ] Can document incidents properly

## 📚 Additional Resources

### Quick Reference Cards
- Dashboard button functions
- Alert severity meanings
- Common troubleshooting steps
- Emergency contact information

### Documentation Links
- Production Deployment Guide
- API Reference Documentation
- System Architecture Overview
- Troubleshooting Knowledge Base

### Training Materials
- Video tutorials (if available)
- Hands-on lab exercises
- Case study examples
- Best practices guide