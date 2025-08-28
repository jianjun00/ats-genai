# WSL System Monitor with Slack Alerts

A comprehensive system monitoring solution for WSL environments with real-time Slack notifications for system stress conditions.

## 🚀 Features

### **System Monitoring**
- **CPU Usage**: Real-time CPU utilization with configurable thresholds
- **Memory Usage**: RAM and swap monitoring with available memory tracking
- **Disk Usage**: Storage monitoring with free space alerts
- **Network Activity**: Network I/O monitoring
- **Process Monitoring**: Process count and Docker container health
- **Database Health**: PostgreSQL connection monitoring
- **ATS-Specific**: Backfill process monitoring and data directory size tracking

### **Slack Integration**
- **Rich Notifications**: Formatted messages with system details and color coding
- **Rate Limiting**: Prevents alert spam (configurable intervals)
- **Severity Levels**: Info, Warning, Critical, and Recovery alerts
- **Recovery Alerts**: Automatic notifications when issues resolve

### **Advanced Features**
- **Intelligent Thresholds**: Configurable warning and critical levels
- **Historical Data**: Metrics logging for trend analysis
- **Auto-Recovery**: Service restarts automatically on failure
- **WSL Optimized**: Designed specifically for WSL environments

---

## 📦 Installation

### **Prerequisites**
```bash
# Install required Python packages
pip3 install --user psutil requests

# Ensure monitoring directories exist
mkdir -p /mnt/d/ats-logs/monitoring
```

### **1. Official ATS Slack Integration**

**✅ Pre-configured Slack webhook for ATS alerts:**
```
https://hooks.slack.com/services/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
```

**Target Channel**: `#ats-alerts` (configured for ATS team notifications)

*Note: This is the official ATS monitoring webhook - no setup required!*

### **2. Install as System Service**
```bash
# Use the official ATS Slack webhook URL
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/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"

# Run installation script
cd /home/jianjun/ats-genai-admin/scripts/monitoring
./install_monitor_service.sh
```

### **3. Manual Installation (Alternative)**
```bash
# Test the monitor manually
python3 wsl_system_monitor.py \\
    --slack-webhook="https://hooks.slack.com/services/YOUR/WEBHOOK/URL" \\
    --config-file=monitor_config.json \\
    --test-alert

# Run continuously (manual)
python3 wsl_system_monitor.py \\
    --slack-webhook="https://hooks.slack.com/services/YOUR/WEBHOOK/URL" \\
    --config-file=monitor_config.json \\
    --interval=60
```

---

## ⚙️ Configuration

### **Default Thresholds** (`monitor_config.json`)
```json
{
  "thresholds": {
    "cpu_warning": 75.0,      // 75% CPU usage warning
    "cpu_critical": 90.0,     // 90% CPU usage critical
    "memory_warning": 80.0,   // 80% memory usage warning
    "memory_critical": 92.0,  // 92% memory usage critical
    "disk_warning": 80.0,     // 80% disk usage warning
    "disk_critical": 90.0,    // 90% disk usage critical
    "disk_free_gb": 10.0,     // Less than 10GB free space
    "max_processes": 400,     // Too many processes running
    "max_db_connections": 80, // PostgreSQL connection limit
    "ats_data_max_gb": 1000.0 // ATS data directory size limit
  },
  "alert_settings": {
    "rate_limit_minutes": 15, // Minimum time between similar alerts
    "monitoring_interval_seconds": 60
  }
}
```

### **Customizing Thresholds**
Edit `monitor_config.json` to adjust thresholds for your environment:

```bash
# Edit configuration
nano /home/jianjun/ats-genai-admin/scripts/monitoring/monitor_config.json

# Restart service to apply changes
sudo systemctl restart ats-wsl-monitor
```

---

## 🧪 Testing

### **Run Test Suite**
```bash
cd /home/jianjun/ats-genai-admin/scripts/monitoring

# Run all tests
python3 test_monitoring.py --slack-webhook="YOUR_WEBHOOK_URL"

# Run specific tests
python3 test_monitoring.py --slack-webhook="YOUR_WEBHOOK_URL" --test=connectivity
python3 test_monitoring.py --slack-webhook="YOUR_WEBHOOK_URL" --test=alerts
python3 test_monitoring.py --slack-webhook="YOUR_WEBHOOK_URL" --test=stress
```

### **Expected Test Output**
```
🧪 WSL System Monitor Test Suite
==================================================
🔗 Testing Slack webhook connectivity...
✅ Slack webhook connectivity test PASSED

🚨 Testing different alert severity levels...
  📤 Sending info alert...
    ✅ info alert sent
  📤 Sending warning alert...
    ✅ warning alert sent
  📤 Sending critical alert...
    ✅ critical alert sent

📊 Alert test results: 4/4 alerts sent successfully
🎉 All tests passed! Your monitoring system is ready!
```

---

## 🔧 Service Management

### **Service Commands**
```bash
# Check service status
sudo systemctl status ats-wsl-monitor

# Start/stop/restart service
sudo systemctl start ats-wsl-monitor
sudo systemctl stop ats-wsl-monitor  
sudo systemctl restart ats-wsl-monitor

# Enable/disable auto-start
sudo systemctl enable ats-wsl-monitor
sudo systemctl disable ats-wsl-monitor

# View live logs
journalctl -u ats-wsl-monitor -f

# View recent logs
journalctl -u ats-wsl-monitor -n 50
```

### **Log Files**
- **Service Logs**: `journalctl -u ats-wsl-monitor`
- **Application Logs**: `/mnt/d/ats-logs/monitoring/wsl_system_monitor.log`
- **Metrics History**: `/mnt/d/ats-logs/monitoring/system_metrics_history.jsonl`

---

## 📊 Alert Examples

### **CPU Warning Alert**
```
🟡 WARNING: High CPU Usage
CPU usage at 82.3% (threshold: 75.0%)

System: ats-dev-machine
CPU: 82.3%
Memory: 65.2% (4,256MB available)
Disk: 45.1% (234.5GB free)
ATS Backfill: ✅ Active
```

### **Memory Critical Alert**
```
🔴 CRITICAL: Critical Memory Usage  
Memory usage at 94.1% with only 512MB available

System: ats-dev-machine
CPU: 45.2%
Memory: 94.1% (512MB available)
Disk: 67.8% (123.4GB free)
ATS Backfill: ✅ Active
DB Status: connected
```

### **Recovery Alert**
```
✅ RECOVERY: Memory Usage Recovered
System has recovered from memory critical condition

Memory usage is now 72.3% with 3,456MB available
```

---

## 🔍 Monitoring Metrics

### **System Metrics Tracked**
- **CPU**: Usage percentage, core count, load averages
- **Memory**: Total, used, available, swap usage
- **Disk**: Usage percentage, free space in GB
- **Network**: Bytes sent/received
- **Processes**: Total process count
- **Docker**: Container count and running status

### **ATS-Specific Metrics**
- **Backfill Status**: Polygon minute bar backfill active/inactive
- **Data Directory Size**: Total size of `/mnt/d/ats-data` in GB
- **Database Health**: PostgreSQL connection count and status

### **Historical Data**
Metrics are logged to `/mnt/d/ats-logs/monitoring/system_metrics_history.jsonl`:
```json
{
  "timestamp": "2025-08-28T02:30:00",
  "hostname": "ats-dev-machine", 
  "cpu_percent": 45.2,
  "memory_percent": 72.3,
  "disk_percent": 67.8,
  "ats_backfill_active": true,
  "ats_data_size_gb": 245.7
}
```

---

## 🚨 Alert Types and Conditions

### **CPU Alerts**
- **Warning**: CPU usage ≥ 75% for sustained period
- **Critical**: CPU usage ≥ 90% 

### **Memory Alerts**  
- **Warning**: Memory usage ≥ 80% OR available memory < 1GB
- **Critical**: Memory usage ≥ 92% OR available memory < 500MB

### **Disk Alerts**
- **Warning**: Disk usage ≥ 80% OR free space < 10GB
- **Critical**: Disk usage ≥ 90% OR free space < 5GB

### **Process Alerts**
- **Warning**: More than 400 processes running
- **Database**: More than 80 PostgreSQL connections

### **ATS-Specific Alerts**
- **Warning**: ATS data directory > 1TB
- **Info**: Backfill process started/stopped

---

## 🔧 Troubleshooting

### **Service Won't Start**
```bash
# Check service status
sudo systemctl status ats-wsl-monitor

# Check detailed logs
journalctl -u ats-wsl-monitor -n 50

# Common issues:
# 1. Invalid Slack webhook URL
# 2. Missing Python packages
# 3. Permission issues with log directories
```

### **No Slack Notifications**
```bash
# Test webhook manually
python3 test_monitoring.py --slack-webhook="YOUR_URL" --test=connectivity

# Check logs for errors
tail -f /mnt/d/ats-logs/monitoring/wsl_system_monitor.log

# Verify webhook URL format
curl -X POST -H 'Content-type: application/json' \\
    --data '{"text":"Test message"}' \\
    YOUR_WEBHOOK_URL
```

### **False Alerts**
```bash
# Adjust thresholds in configuration
nano monitor_config.json

# Increase rate limiting
# Set "rate_limit_minutes": 30 for less frequent alerts

# Restart service to apply changes  
sudo systemctl restart ats-wsl-monitor
```

---

## 📈 Advanced Usage

### **Custom Thresholds for Different Environments**
```json
// Development environment - more relaxed
{
  "thresholds": {
    "cpu_warning": 85.0,
    "memory_warning": 90.0,
    "disk_warning": 90.0
  }
}

// Production environment - strict monitoring
{
  "thresholds": {
    "cpu_warning": 60.0,
    "memory_warning": 70.0,
    "disk_warning": 75.0
  }
}
```

### **Integration with Other Tools**
```bash
# Export metrics to external monitoring systems
tail -f /mnt/d/ats-logs/monitoring/system_metrics_history.jsonl | \\
    while read line; do
        curl -X POST -H "Content-Type: application/json" \\
            -d "$line" https://your-metrics-endpoint.com/api/metrics
    done
```

---

## 🛡️ Security Considerations

- **Webhook URL**: Keep your Slack webhook URL secure and don't commit it to version control
- **Service Isolation**: The service runs with limited privileges and resource constraints
- **Log Rotation**: Logs are automatically rotated to prevent disk space issues
- **Network Access**: Only outbound HTTPS connections to Slack are required

---

## 📞 Support

- **Logs**: Check `/mnt/d/ats-logs/monitoring/` for detailed logs
- **Configuration**: Modify `monitor_config.json` for custom settings  
- **Testing**: Use `test_monitoring.py` to validate setup
- **Service Management**: Standard systemd commands for service control

The WSL System Monitor is now ready to keep watch over your ATS development environment! 🚀