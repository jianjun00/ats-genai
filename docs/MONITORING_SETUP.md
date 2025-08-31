# ATS System Monitoring Setup

**✅ UPDATED 2025-08-31**: Simplified monitoring system with hourly Slack alerts

## 🚀 Official Slack Integration

**Slack Webhook URL**: `https://hooks.slack.com/services/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`

**Target Channel**: `#ats-alerts`

## 📦 Quick Setup (No Dependencies Required)

### 1. Test the System
```bash
cd /home/jianjun/ats-genai-model/scripts/monitoring

# Send test alert to Slack
python3 simple_wsl_monitor.py --test
```

### 2. Check Current System Status
```bash
# View detailed system status 
python3 simple_wsl_monitor.py --status
```

### 3. Start Hourly Monitoring
```bash
# Option A: Run in background
nohup python3 simple_wsl_monitor.py --hourly > /dev/null 2>&1 &

# Option B: Install as systemd service (requires sudo)
./install_hourly_monitor.sh
```

### 4. Frequent Monitoring (Testing/Debugging)
```bash
# Send updates every 5 minutes (for testing system stress)
./start_frequent_monitoring.sh
```

## 📊 Monitoring Features

**System Metrics Tracked:**
- **CPU Usage & Load Average**: Intel Core Ultra 7 265F utilization
- **Memory Usage**: 31Gi total RAM monitoring  
- **Disk Usage**: 1007G storage monitoring
- **Docker Containers**: ATS container health status
- **PostgreSQL**: Database connectivity and performance
- **ATS Processes**: Backfill job monitoring
- **Network & Process Count**: System resource usage

### Sample Hourly Status Report
```
🖥️ Host: game
⏱️ Time: 2025-08-31 15:47:40
📊 Uptime: 52 min
📈 Load Average: 1.48, 0.95, 0.58

💾 Memory: Total: 31Gi, Used: 9.5Gi, Available: 21Gi
💿 Disk: Size: 1007G, Used: 206G (22%), Available: 750G
⚙️ CPU: Intel(R) Core(TM) Ultra 7 265F

🐳 Docker: 3 containers running
🗄️ PostgreSQL: ✅ Connected
📊 ATS Backfill: ✅ Active (1 processes)

Running Containers:
ats-intg-analytics   Up 54 minutes (unhealthy)
ats-intg-postgres    Up 54 minutes (healthy)
ats-dev-postgres     Up 54 minutes (healthy)
```

## 🔧 Service Management

**For systemd service (if installed):**
```bash
# Check service status
sudo systemctl status ats-wsl-hourly-monitor

# View logs
journalctl -u ats-wsl-hourly-monitor -f

# Restart service
sudo systemctl restart ats-wsl-hourly-monitor
```

**For background process:**
```bash
# Check if running
ps aux | grep simple_wsl_monitor

# Stop background monitoring
pkill -f simple_wsl_monitor
```

## 📱 Alert Capabilities

**Current Implementation:**
- ✅ **Hourly Status Reports**: System health every hour
- ✅ **Test Alerts**: On-demand test messages
- ✅ **System Monitoring**: All critical metrics tracked
- ⚠️ **Stress Alerts**: Available in advanced monitor (requires psutil)

**Future Enhancement** (when psutil is available):
- CPU/Memory threshold alerts
- Recovery notifications  
- Rate-limited alerting
- Historical trending

## 🔍 Why WSL Crash Wasn't Detected

**Root Cause Analysis:**
1. ❌ No active monitoring service was running
2. ❌ Original monitor had dependency issues (psutil unavailable)
3. ❌ No systemd service configured for automatic startup

**✅ Fixed**: New simplified monitor works without external dependencies and provides reliable hourly updates!