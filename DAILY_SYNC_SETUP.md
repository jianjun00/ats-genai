# 🚀 ATS Daily Prices Sync Setup

## ✅ **Setup Complete - Ready to Install**

I've configured automated daily prices sync for ATS-INTG with comprehensive monitoring.

### 📋 **What's Been Created**

**Systemd Service Files:**
- `config/systemd/ats-daily-sync.service` - Daily sync service
- `config/systemd/ats-daily-sync.timer` - Scheduled timer (Mon-Fri 1:00 AM)

**Setup Scripts:**
- `scripts/setup_daily_sync.sh` - Installation script (requires sudo)
- `scripts/test_daily_sync.py` - Validation and testing

**Monitoring Configuration:**
- `config/monitoring/daily_sync_alerts.yml` - Alert thresholds and rules
- Enhanced services with Prometheus metrics integration
- Pushgateway integration at `localhost:9091`

## 🎯 **Installation Steps**

### **1. Install Systemd Services**
```bash
# Run the setup script (requires sudo password)
sudo ./scripts/setup_daily_sync.sh
```

### **2. Start the Timer**
```bash
# Enable and start the daily sync timer
sudo systemctl start ats-daily-sync.timer

# Verify it's running
systemctl status ats-daily-sync.timer
```

### **3. Test the Setup**
```bash
# Test the service manually (optional)
sudo systemctl start ats-daily-sync.service

# Check logs
tail -f /mnt/d/ats-logs/daily-sync.log
```

## 📊 **What Gets Synced**

**Daily Schedule (Mon-Fri 1:00 AM):**
1. **EODHD**: DEV → INTG database sync
2. **Tiingo**: DEV → INTG database sync
3. **Polygon**: DEV → INTG database sync

**Data Flow:**
- Source: `localhost:3432` (DEV database)
- Target: `localhost:4432` (INTG database)
- Method: Incremental sync with `ON CONFLICT DO NOTHING`

## 📈 **Monitoring & Alerts**

**Prometheus Metrics:**
- `ats_daily_prices_sync_symbols_processed_total` - Symbols synced per vendor
- `ats_daily_prices_sync_prices_processed_total` - Price records synced
- `ats_daily_prices_sync_success_rate` - Success rate per vendor
- `ats_daily_prices_sync_duration_seconds` - Sync duration

**Grafana Dashboard:**
- **URL**: http://10.0.0.79:4002/d/a94a33f2-aeea-4b56-93c4-4d22a0cf1c2b
- **Features**: Real-time sync metrics, success rates, performance tracking

**Logs:**
- **Main Log**: `/mnt/d/ats-logs/daily-sync.log`
- **Error Log**: `/mnt/d/ats-logs/daily-sync-error.log`

## 🔧 **Management Commands**

```bash
# Check timer status
systemctl list-timers ats-daily-sync.timer

# View service status
systemctl status ats-daily-sync.service

# View logs
journalctl -u ats-daily-sync.service -f

# Stop/start timer
sudo systemctl stop ats-daily-sync.timer
sudo systemctl start ats-daily-sync.timer

# Disable automatic sync
sudo systemctl disable ats-daily-sync.timer
```

## 🎉 **Benefits**

✅ **Automated**: Runs Monday-Friday at 1:00 AM
✅ **Monitored**: Full Prometheus metrics and Grafana dashboard
✅ **Safe**: Uses `ON CONFLICT DO NOTHING` - never deletes existing data
✅ **Logged**: Comprehensive logging for troubleshooting
✅ **Efficient**: Only syncs new/changed records
✅ **Resilient**: Systemd restart policies and error handling

---

**🚀 Ready to install? Run: `sudo ./scripts/setup_daily_sync.sh`**