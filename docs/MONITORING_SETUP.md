# ATS System Monitoring Setup

Official configuration for ATS WSL system monitoring with Slack alerts.

## 🚀 Official Slack Integration

**Slack Webhook URL**: `https://hooks.slack.com/services/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`

**Target Channel**: `#ats-alerts` (or configured channel)

## 📦 Quick Setup

### 1. Install Dependencies
```bash
sudo apt install python3-psutil python3-requests -y
```

### 2. Test the System
```bash
cd /home/jianjun/ats-genai-admin/scripts/monitoring

# Send test alert
python3 -c "
import requests
import json
from datetime import datetime
import socket

webhook_url = 'https://hooks.slack.com/services/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'

payload = {
    'username': 'ATS System Monitor',
    'icon_emoji': ':warning:',
    'text': f'🧪 Test alert from {socket.gethostname()} at {datetime.now().strftime(\"%H:%M:%S\")} - System monitoring is active!'
}

response = requests.post(webhook_url, json=payload)
print('✅ Test sent!' if response.status_code == 200 else f'❌ Failed: {response.status_code}')
"
```

### 3. Install as Service
```bash
cd /home/jianjun/ats-genai-admin/scripts/monitoring

# Set the official webhook URL
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/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"

# Install service
./install_monitor_service.sh
```

### 4. Manual Run (Alternative)
```bash
cd /home/jianjun/ats-genai-admin/scripts/monitoring

python3 wsl_system_monitor.py \
    --slack-webhook="https://hooks.slack.com/services/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" \
    --config-file=monitor_config.json \
    --interval=60
```

## 📊 Alert Examples

With the current system status, you would receive alerts like:

### Memory Critical Alert (Current: 93% usage)
```
🔴 CRITICAL: Critical Memory Usage
Memory usage at 93.0% with only 301MB available

System: game
CPU: [current]%
Memory: 93.0% (301MB available) 
Disk: 44.0% (537GB free)
ATS Backfill: ✅ Active
DB Status: connected
```

## 🔧 Service Management

```bash
# Check if service is running
sudo systemctl status ats-wsl-monitor

# View live alerts
journalctl -u ats-wsl-monitor -f

# Restart service
sudo systemctl restart ats-wsl-monitor
```

## 📱 Expected Slack Notifications

The system will send alerts for:
- **CPU Usage** > 75% (warning) / 90% (critical)
- **Memory Usage** > 80% (warning) / 92% (critical) ⚠️ *Current system would alert*
- **Disk Usage** > 80% (warning) / 90% (critical)
- **Process Issues**: Too many processes, database connections
- **ATS Specific**: Backfill status, data directory size
- **Recovery Alerts**: When conditions return to normal

## 🔍 Current System Status

Based on latest check:
- **✅ Hostname**: `game`
- **⚠️ Memory**: 93% used (CRITICAL - would trigger alert)
- **✅ Disk**: 44% used (537GB free)
- **✅ Monitoring**: Ready and configured

The monitoring system is now ready to protect your ATS development environment!